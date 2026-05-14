package queue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hecate/navidrome-jukebox/internal/models"
	_ "github.com/mattn/go-sqlite3"
)

// Default radio config values used when nothing has been persisted yet.
const (
	defaultQueueThreshold = 5
	defaultBatchSize      = 10
)

// How long a track stays in the "recently played" exclusion window used by
// radio fill. In-memory only — restarts wipe it, which is acceptable.
const recencyWindow = 6 * time.Hour

func defaultRadioConfig() models.RadioConfig {
	return models.RadioConfig{
		Enabled:        false,
		QueueThreshold: defaultQueueThreshold,
		BatchSize:      defaultBatchSize,
	}
}

// Engine manages the playback queue and state.
// The queue holds upcoming tracks only; nowPlaying is set by the server
// based on what the renderer is actually playing.
type Engine struct {
	mu         sync.RWMutex
	queue      []models.QueueItem
	nowPlaying *models.QueueItem
	isRunning  bool
	radioCfg   models.RadioConfig
	syncState  models.SyncState
	renderer   models.PlaybackState
	upnpStatus string
	db         *sql.DB

	// recent maps trackID -> time it last became nowPlaying. Used to suppress
	// re-queueing recently-heard songs in radio fill.
	recent map[string]time.Time

	// Called when radio mode is on and queue depth <= threshold.
	// Always invoked off the request goroutine because fills can hit Navidrome
	// for similarity lookups, which can take seconds.
	RadioFillFunc func()
	radioFilling  bool

	// SSE subscribers
	subMu       sync.RWMutex
	subscribers map[chan []byte]struct{}
}

// triggerFillAsync runs RadioFillFunc in a goroutine, single-flighted so
// repeated near-simultaneous triggers (e.g. Next + Remove) don't stack.
// Broadcasts radioFilling=true at start and false at completion.
func (e *Engine) triggerFillAsync() {
	if e.RadioFillFunc == nil {
		return
	}
	e.mu.Lock()
	if e.radioFilling {
		e.mu.Unlock()
		return
	}
	e.radioFilling = true
	e.broadcastLocked()
	e.mu.Unlock()
	go func() {
		defer func() {
			e.mu.Lock()
			e.radioFilling = false
			e.broadcastLocked()
			e.mu.Unlock()
		}()
		e.RadioFillFunc()
	}()
}

// NewEngine creates a new queue engine
func NewEngine(dbPath string) (*Engine, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			track_id TEXT NOT NULL,
			title TEXT NOT NULL,
			artist TEXT NOT NULL,
			album TEXT NOT NULL,
			year INTEGER,
			duration INTEGER NOT NULL,
			cover_art TEXT,
			added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			position INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_queue_position ON queue(position);
		CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	e := &Engine{
		queue:       make([]models.QueueItem, 0),
		isRunning:   false,
		radioCfg:    defaultRadioConfig(),
		db:          db,
		subscribers: make(map[chan []byte]struct{}),
		recent:      make(map[string]time.Time),
	}

	if err := e.loadQueue(); err != nil {
		return nil, fmt.Errorf("failed to load queue: %w", err)
	}

	// Load persisted radio config (JSON). Falls back to legacy radio_mode bool
	// for users upgrading from a pre-config build.
	var raw string
	if err := db.QueryRow("SELECT value FROM settings WHERE key = 'radio_config'").Scan(&raw); err == nil {
		var cfg models.RadioConfig
		if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
			if cfg.QueueThreshold <= 0 {
				cfg.QueueThreshold = defaultQueueThreshold
			}
			if cfg.BatchSize <= 0 {
				cfg.BatchSize = defaultBatchSize
			}
			e.radioCfg = cfg
		}
	} else {
		var radioVal int
		if err := db.QueryRow("SELECT value FROM settings WHERE key = 'radio_mode'").Scan(&radioVal); err == nil {
			e.radioCfg.Enabled = radioVal == 1
		}
	}

	return e, nil
}

// loadQueue loads the queue from database
func (e *Engine) loadQueue() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rows, err := e.db.Query("SELECT track_id, title, artist, album, year, duration, cover_art, position FROM queue ORDER BY position")
	if err != nil {
		return err
	}
	defer rows.Close()

	e.queue = make([]models.QueueItem, 0)
	for rows.Next() {
		var item models.QueueItem
		var year sql.NullInt64
		var position int
		err := rows.Scan(&item.ID, &item.Title, &item.Artist, &item.Album, &year, &item.Duration, &item.CoverArt, &position)
		if err != nil {
			return err
		}
		if year.Valid {
			item.Year = int(year.Int64)
		}
		item.AddedAt = time.Time{}
		item.AddedBy = "system"
		e.queue = append(e.queue, item)
	}

	return rows.Err()
}

// saveQueue persists the queue to database
func (e *Engine) saveQueue() error {
	_, err := e.db.Exec("DELETE FROM queue")
	if err != nil {
		return err
	}

	stmt, err := e.db.Prepare("INSERT INTO queue (track_id, title, artist, album, year, duration, cover_art, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, item := range e.queue {
		year := sql.NullInt64{Int64: int64(item.Year), Valid: item.Year > 0}
		_, err := stmt.Exec(item.ID, item.Title, item.Artist, item.Album, year, item.Duration, item.CoverArt, i)
		if err != nil {
			return err
		}
	}

	return nil
}

// Add adds a track to the end of the queue
func (e *Engine) Add(item models.QueueItem) {
	e.mu.Lock()
	item.AddedAt = time.Now()
	item.AddedBy = "system"
	e.queue = append(e.queue, item)
	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// InsertNext adds a track at position 0 (next up)
func (e *Engine) InsertNext(item models.QueueItem) {
	e.mu.Lock()
	item.AddedAt = time.Now()
	item.AddedBy = "system"
	e.queue = append([]models.QueueItem{item}, e.queue...)
	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// PopNext removes and returns queue[0], or nil if empty
func (e *Engine) PopNext() *models.QueueItem {
	e.mu.Lock()
	if len(e.queue) == 0 {
		e.mu.Unlock()
		return nil
	}

	item := e.queue[0]
	e.queue = e.queue[1:]
	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
	e.checkRadioFill()
	return &item
}

// Peek returns queue[0] without removing it, or nil if empty
func (e *Engine) Peek() *models.QueueItem {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.queue) == 0 {
		return nil
	}
	item := e.queue[0]
	return &item
}

// RemoveByTrackID removes the first occurrence of a track ID from the queue
func (e *Engine) RemoveByTrackID(trackID string) bool {
	e.mu.Lock()
	for i, item := range e.queue {
		if item.ID == trackID {
			e.queue = append(e.queue[:i], e.queue[i+1:]...)
			if err := e.saveQueue(); err != nil {
				fmt.Printf("Failed to save queue: %v\n", err)
			}
			e.broadcastLocked()
			e.mu.Unlock()
			e.checkRadioFill()
			return true
		}
	}
	e.mu.Unlock()
	return false
}

// Reorder moves queue[from] to position [to], but only if queue[from].ID matches trackID.
// Returns false if the index is out of range or the track ID doesn't match (stale drag).
func (e *Engine) Reorder(from, to int, trackID string) bool {
	e.mu.Lock()
	if from < 0 || from >= len(e.queue) || to < 0 || to >= len(e.queue) {
		e.mu.Unlock()
		return false
	}
	if e.queue[from].ID != trackID {
		e.mu.Unlock()
		return false
	}

	item := e.queue[from]
	// Remove from old position
	e.queue = append(e.queue[:from], e.queue[from+1:]...)
	// Insert at new position
	e.queue = append(e.queue[:to], append([]models.QueueItem{item}, e.queue[to:]...)...)

	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
	return true
}

// Remove removes a track from the queue by index
func (e *Engine) Remove(idx int) bool {
	e.mu.Lock()
	if idx < 0 || idx >= len(e.queue) {
		e.mu.Unlock()
		return false
	}

	e.queue = append(e.queue[:idx], e.queue[idx+1:]...)
	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
	e.checkRadioFill()
	return true
}

// Clear removes all tracks from the queue
func (e *Engine) Clear() {
	e.mu.Lock()
	e.queue = make([]models.QueueItem, 0)
	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// Shuffle randomizes the queue
func (e *Engine) Shuffle() {
	e.mu.Lock()
	if len(e.queue) <= 1 {
		e.mu.Unlock()
		return
	}

	rand.Shuffle(len(e.queue), func(i, j int) {
		e.queue[i], e.queue[j] = e.queue[j], e.queue[i]
	})

	if err := e.saveQueue(); err != nil {
		fmt.Printf("Failed to save queue: %v\n", err)
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// NowPlaying returns the currently playing track
func (e *Engine) NowPlaying() *models.QueueItem {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.nowPlaying
}

// SetNowPlaying sets the currently playing track (derived from renderer state).
// Also stamps the track's ID into the recency cache so radio fill won't
// re-queue it within the recencyWindow.
func (e *Engine) SetNowPlaying(item *models.QueueItem) {
	e.mu.Lock()
	e.nowPlaying = item
	if item != nil && item.ID != "" {
		e.recent[item.ID] = time.Now()
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// RecentIDs returns the set of trackIDs played within recencyWindow.
// Prunes expired entries as a side effect.
func (e *Engine) RecentIDs() map[string]struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	cutoff := time.Now().Add(-recencyWindow)
	out := make(map[string]struct{}, len(e.recent))
	for id, t := range e.recent {
		if t.Before(cutoff) {
			delete(e.recent, id)
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

// RecentCount returns how many tracks are currently in the recency window.
func (e *Engine) RecentCount() int {
	return len(e.RecentIDs())
}

// IsRunning returns whether playback is active
func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isRunning
}

// SetRunning sets the running state
func (e *Engine) SetRunning(running bool) {
	e.mu.Lock()
	e.isRunning = running
	if !running {
		e.nowPlaying = nil
	}
	e.broadcastLocked()
	e.mu.Unlock()
}

// RadioConfig returns a copy of the current radio configuration.
func (e *Engine) RadioConfig() models.RadioConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.radioCfg
}

// SetRadioConfig updates the radio configuration, persists it, broadcasts the
// new state, and triggers a fill if the queue is already at/below threshold.
func (e *Engine) SetRadioConfig(cfg models.RadioConfig) {
	if cfg.QueueThreshold <= 0 {
		cfg.QueueThreshold = defaultQueueThreshold
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	e.mu.Lock()
	e.radioCfg = cfg
	if raw, err := json.Marshal(cfg); err == nil {
		e.db.Exec("INSERT OR REPLACE INTO settings (key, value) VALUES ('radio_config', ?)", string(raw))
	}
	e.broadcastLocked()
	needFill := cfg.Enabled && len(e.queue) <= cfg.QueueThreshold && e.RadioFillFunc != nil
	e.mu.Unlock()
	if needFill {
		e.triggerFillAsync()
	}
}

// checkRadioFill triggers the fill callback if radio mode is on and queue is at/below threshold.
// Runs the fill on a background goroutine — similarity-pool fills make Navidrome
// HTTP calls and can take seconds; we never want them blocking a request.
// Must NOT be called while e.mu is held.
func (e *Engine) checkRadioFill() {
	e.mu.RLock()
	needFill := e.radioCfg.Enabled && len(e.queue) <= e.radioCfg.QueueThreshold && e.RadioFillFunc != nil
	e.mu.RUnlock()
	if needFill {
		e.triggerFillAsync()
	}
}

// SetSyncState updates the library sync state and broadcasts.
func (e *Engine) SetSyncState(s models.SyncState) {
	e.mu.Lock()
	e.syncState = s
	e.broadcastLocked()
	e.mu.Unlock()
}

// SyncState returns the current sync state.
func (e *Engine) SyncState() models.SyncState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.syncState
}

// QueueSnapshot returns a copy of the current queue safe to inspect outside the lock.
func (e *Engine) QueueSnapshot() []models.QueueItem {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]models.QueueItem, len(e.queue))
	copy(out, e.queue)
	return out
}

// SetRenderer updates the renderer playback state and broadcasts
func (e *Engine) SetRenderer(state models.PlaybackState) {
	e.mu.Lock()
	e.renderer = state
	e.broadcastLocked()
	e.mu.Unlock()
}

// SetUPnPStatus updates the UPnP connection status and broadcasts
func (e *Engine) SetUPnPStatus(status string) {
	e.mu.Lock()
	e.upnpStatus = status
	e.broadcastLocked()
	e.mu.Unlock()
}

// QueueLen returns the number of items in the queue
func (e *Engine) QueueLen() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.queue)
}

// State returns the current system state
func (e *Engine) State() models.SystemState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	queue := make([]models.QueueItem, len(e.queue))
	copy(queue, e.queue)

	return models.SystemState{
		Queue:        queue,
		NowPlaying:   e.nowPlaying,
		IsRunning:    e.isRunning,
		Renderer:     e.renderer,
		RadioMode:    e.radioCfg.Enabled,
		Radio:        e.radioCfg,
		RadioFilling: e.radioFilling,
		Sync:         e.syncState,
		UPnPStatus:   e.upnpStatus,
	}
}

// broadcastLocked sends current state to all subscribers.
// Must be called while e.mu is held.
func (e *Engine) broadcastLocked() {
	state := models.SystemState{
		Queue:        make([]models.QueueItem, len(e.queue)),
		NowPlaying:   e.nowPlaying,
		IsRunning:    e.isRunning,
		Renderer:     e.renderer,
		RadioMode:    e.radioCfg.Enabled,
		Radio:        e.radioCfg,
		RadioFilling: e.radioFilling,
		Sync:         e.syncState,
		UPnPStatus:   e.upnpStatus,
	}
	copy(state.Queue, e.queue)
	data, _ := json.Marshal(state)

	e.subMu.RLock()
	for ch := range e.subscribers {
		select {
		case ch <- data:
		default:
		}
	}
	e.subMu.RUnlock()
}

// Subscribe returns a new channel that receives state broadcasts
func (e *Engine) Subscribe() chan []byte {
	ch := make(chan []byte, 16)
	e.subMu.Lock()
	e.subscribers[ch] = struct{}{}
	e.subMu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel
func (e *Engine) Unsubscribe(ch chan []byte) {
	e.subMu.Lock()
	delete(e.subscribers, ch)
	e.subMu.Unlock()
}

// Close closes the database connection
func (e *Engine) Close() error {
	return e.db.Close()
}
