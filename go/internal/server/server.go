package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/hecate/navidrome-jukebox/internal/lastfm"
	"github.com/hecate/navidrome-jukebox/internal/library"
	"github.com/hecate/navidrome-jukebox/internal/models"
	"github.com/hecate/navidrome-jukebox/internal/navidrome"
	"github.com/hecate/navidrome-jukebox/internal/queue"
	"github.com/hecate/navidrome-jukebox/internal/upnp"
)

// UPnP connection states
const (
	UPnPStatusUnconnected = "unconnected"
	UPnPStatusConnecting  = "connecting"
	UPnPStatusConnected   = "connected"
)

// Server holds all application state
type Server struct {
	queueEngine *queue.Engine
	lib         *library.Library
	navidrome   *navidrome.Client
	upnpClient  *upnp.Client
	upnpControl *upnp.ControlPoint
	upnpStatus  string
	upnpMu      sync.RWMutex

	// Playback tracking
	playbackMu   sync.Mutex
	lastTrackURI string // last URI seen from the renderer

	// Config
	navidromeBaseURL string
	rendererName     string

	// Last.fm (nil if not configured)
	lastfmScrobbler *lastfm.Scrobbler
}

// NewServer creates a new server instance
func NewServer(navidromeURL, navidromeUser, navidromePass, rendererAddr, lastfmAPIKey, lastfmAPISecret string) (*Server, error) {
	const dbPath = "/data/app.db"

	// Create queue engine
	qe, err := queue.NewEngine(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue engine: %w", err)
	}

	// Create Navidrome client
	nvClient := navidrome.NewClient(navidromeURL, navidromeUser, navidromePass)

	// Create library manager
	lib, err := library.NewLibrary(dbPath, nvClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create library: %w", err)
	}

	// Create UPnP client
	upnpClient := upnp.NewClient(rendererAddr)

	srv := &Server{
		queueEngine:      qe,
		lib:              lib,
		navidrome:        nvClient,
		upnpClient:       upnpClient,
		upnpStatus:       UPnPStatusUnconnected,
		navidromeBaseURL: navidromeURL,
		rendererName:     rendererAddr,
	}
	qe.SetUPnPStatus(UPnPStatusUnconnected)

	// Last.fm scrobbling (optional)
	if lastfmAPIKey != "" {
		lfmClient := lastfm.NewClient(lastfmAPIKey, lastfmAPISecret)
		lfmStore, err := lastfm.NewStore(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create lastfm store: %w", err)
		}
		srv.lastfmScrobbler = lastfm.NewScrobbler(lfmClient, lfmStore)
	}

	qe.RadioFillFunc = srv.radioFill

	return srv, nil
}

// StartUPnP discovers and connects to the renderer
func (s *Server) StartUPnP() error {
	s.setUPnPStatus(UPnPStatusConnecting)

	log.Printf("Discovering UPnP renderer matching %q...", s.rendererName)
	if err := s.upnpClient.Discover(); err != nil {
		s.setUPnPStatus(UPnPStatusUnconnected)
		log.Printf("Failed to discover UPnP renderer: %v", err)
		return fmt.Errorf("failed to discover renderer: %w", err)
	}
	s.upnpControl = s.upnpClient.NewControlPoint()
	s.setUPnPStatus(UPnPStatusConnected)
	log.Printf("Connected to UPnP renderer")

	// Pick up current renderer state (in case it's already playing)
	s.pickUpRendererState()

	return nil
}

// pickUpRendererState checks if the renderer is currently playing and syncs our state
func (s *Server) pickUpRendererState() {
	control := s.upnpControl
	if control == nil {
		return
	}

	transportState, err := control.GetTransportInfo("0")
	if err != nil {
		return
	}

	posInfo, err := control.GetPositionInfo("0")
	if err != nil {
		posInfo = &models.PlaybackState{}
	}
	posInfo.TransportState = transportState

	if vol, volErr := control.GetVolume("0"); volErr == nil {
		posInfo.Volume = vol
	}

	// Derive nowPlaying from the renderer's current URI
	if posInfo.CurrentURI != "" {
		trackID := extractTrackID(posInfo.CurrentURI)
		if trackID != "" {
			trackInfo := s.lib.GetTrackByID(trackID)
			if trackInfo != nil {
				log.Printf("[startup] renderer playing: %q by %s", trackInfo.Title, trackInfo.Artist)
				s.queueEngine.SetNowPlaying(trackInfo)
				s.queueEngine.RemoveByTrackID(trackID)
				if posInfo.Duration == 0 {
					posInfo.Duration = trackInfo.Duration
				}
			}
		}

		s.playbackMu.Lock()
		s.lastTrackURI = posInfo.CurrentURI
		s.playbackMu.Unlock()
	}

	s.queueEngine.SetRenderer(*posInfo)

	if transportState == "PLAYING" || transportState == "PAUSED_PLAYBACK" {
		log.Printf("[startup] renderer is %s, syncing state", transportState)
		s.queueEngine.SetRunning(true)
		// Pre-queue next for gapless
		s.preQueueNext()
	}
}

// setUPnPStatus updates the UPnP status on the server and broadcasts via SSE
func (s *Server) setUPnPStatus(status string) {
	s.upnpMu.Lock()
	s.upnpStatus = status
	s.upnpMu.Unlock()
	s.queueEngine.SetUPnPStatus(status)
}

// GetUPnPStatus returns the current UPnP connection status
func (s *Server) GetUPnPStatus() string {
	s.upnpMu.RLock()
	defer s.upnpMu.RUnlock()
	return s.upnpStatus
}

// ReconnectUPnP retries connecting to the renderer
func (s *Server) ReconnectUPnP() error {
	s.upnpControl = nil
	return s.StartUPnP()
}

// extractTrackID pulls the Navidrome track ID from a stream URL
func extractTrackID(uri string) string {
	// Stream URLs look like: http://host/rest/stream?...&id=TRACKID&...
	if idx := strings.Index(uri, "id="); idx >= 0 {
		id := uri[idx+3:]
		if end := strings.IndexByte(id, '&'); end >= 0 {
			id = id[:end]
		}
		return id
	}
	return ""
}

// StartPlaybackLoop polls the renderer, syncs nowPlaying from its URI,
// removes played tracks from queue, and auto-plays on STOPPED.
func (s *Server) StartPlaybackLoop() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		lastTransportState := ""

		for range ticker.C {
			s.upnpMu.RLock()
			control := s.upnpControl
			s.upnpMu.RUnlock()
			if control == nil {
				continue
			}

			transportState, transErr := control.GetTransportInfo("0")
			if transErr != nil {
				continue
			}

			posInfo, posErr := control.GetPositionInfo("0")
			if posErr != nil {
				posInfo = &models.PlaybackState{}
			}
			posInfo.TransportState = transportState

			if vol, volErr := control.GetVolume("0"); volErr == nil {
				posInfo.Volume = vol
			}

			// --- Sync nowPlaying from renderer's current URI ---
			if posInfo.CurrentURI != "" {
				s.playbackMu.Lock()
				uriChanged := posInfo.CurrentURI != s.lastTrackURI
				s.lastTrackURI = posInfo.CurrentURI
				s.playbackMu.Unlock()

				if uriChanged {
					trackID := extractTrackID(posInfo.CurrentURI)
					if trackID != "" {
						trackInfo := s.lib.GetTrackByID(trackID)
						if trackInfo != nil {
							log.Printf("[playback] now playing: %q by %s", trackInfo.Title, trackInfo.Artist)
							s.queueEngine.SetNowPlaying(trackInfo)
							s.queueEngine.RemoveByTrackID(trackID)

							if s.lastfmScrobbler != nil {
								s.lastfmScrobbler.OnTrackChange(&lastfm.ScrobbleTrack{
									Artist:   trackInfo.Artist,
									Title:    trackInfo.Title,
									Album:    trackInfo.Album,
									Duration: trackInfo.Duration,
								})
							}
						}
					}
					// Pre-queue next track for gapless
					s.preQueueNext()
				}
			}

			// Fill duration from nowPlaying if renderer doesn't report it
			if posInfo.Duration == 0 {
				if np := s.queueEngine.NowPlaying(); np != nil {
					posInfo.Duration = np.Duration
				}
			}

			s.queueEngine.SetRenderer(*posInfo)

			if s.lastfmScrobbler != nil && transportState == "PLAYING" {
				s.lastfmScrobbler.OnPositionUpdate(posInfo.Position, posInfo.Duration)
			}

			// --- Auto-play on STOPPED ---
			if lastTransportState != "STOPPED" && transportState == "STOPPED" {
				s.playNextInQueue()
			}
			lastTransportState = transportState
		}
	}()
}

// playNextInQueue pops queue[0] and plays it, or stops if empty
func (s *Server) playNextInQueue() {
	next := s.queueEngine.PopNext()
	if next != nil {
		s.queueEngine.SetRunning(true)
		if err := s.playTrack(next); err != nil {
			log.Printf("[playback] error playing next: %v", err)
		}
	} else {
		s.queueEngine.SetRunning(false)
	}
}

// preQueueNext sets the next track on the renderer for gapless playback
func (s *Server) preQueueNext() {
	next := s.queueEngine.Peek()
	if next != nil {
		if err := s.queueNextTrack(next); err != nil {
			log.Printf("[playback] failed to pre-queue next: %v", err)
		}
	}
}

// SyncLibrary triggers a library sync, broadcasting progress over SSE.
func (s *Server) SyncLibrary(ctx context.Context) error {
	startCount := s.lib.GetSongCount()
	s.queueEngine.SetSyncState(models.SyncState{
		InProgress: true,
		SongCount:  startCount,
		Synced:     0,
	})
	err := s.lib.Sync(ctx, func(synced int) {
		s.queueEngine.SetSyncState(models.SyncState{
			InProgress: true,
			SongCount:  startCount,
			Synced:     synced,
		})
	})
	s.queueEngine.SetSyncState(models.SyncState{
		InProgress: false,
		SongCount:  s.lib.GetSongCount(),
		Synced:     0,
	})
	return err
}

// GetLibrary returns the library instance
func (s *Server) GetLibrary() *library.Library {
	return s.lib
}

// isTrackLoaded checks if the renderer currently has a track loaded
// by querying transport info for a non-STOPPED/NO_MEDIA state.
func (s *Server) isTrackLoaded(track *models.QueueItem) bool {
	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control == nil {
		return false
	}
	state, err := control.GetTransportInfo("0")
	if err != nil {
		return false
	}
	return state == "PLAYING" || state == "PAUSED_PLAYBACK" || state == "TRANSITIONING"
}

// playTrack plays a track on the UPnP renderer
func (s *Server) playTrack(track *models.QueueItem) error {
	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control == nil {
		return fmt.Errorf("upnp not connected")
	}

	streamURL := s.navidrome.StreamURL(track.ID)
	log.Printf("[playTrack] setting URI for %q by %s", track.Title, track.Artist)

	// Build DIDL-Lite metadata
	meta := upnp.DIDLItem(*track, streamURL)

	// Replace cover art placeholder with actual URL
	if track.CoverArt != "" {
		coverURL := s.navidrome.CoverArtURL(track.CoverArt, 300)
		meta = strings.Replace(meta, fmt.Sprintf("__COVER_ART_%s__", track.CoverArt), coverURL, -1)
	}

	// Set and play
	if err := control.SetAVTransportURI("0", streamURL, meta); err != nil {
		return fmt.Errorf("failed to set URI: %w", err)
	}
	log.Printf("[playTrack] URI set, sending Play")

	if err := control.Play("1"); err != nil {
		return fmt.Errorf("failed to play: %w", err)
	}

	log.Printf("[playTrack] playing")
	return nil
}

// queueNextTrack sets the next track on the renderer for gapless playback
func (s *Server) queueNextTrack(track *models.QueueItem) error {
	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control == nil {
		return fmt.Errorf("upnp not connected")
	}

	streamURL := s.navidrome.StreamURL(track.ID)
	meta := upnp.DIDLItem(*track, streamURL)

	if track.CoverArt != "" {
		coverURL := s.navidrome.CoverArtURL(track.CoverArt, 300)
		meta = strings.Replace(meta, fmt.Sprintf("__COVER_ART_%s__", track.CoverArt), coverURL, -1)
	}

	log.Printf("[queueNext] queuing next: %q by %s", track.Title, track.Artist)
	return control.SetNextAVTransportURI("0", streamURL, meta)
}

// Routes sets up HTTP routes
func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// CORS middleware
	r.Use(func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if origin := r.Header.Get("Origin"); origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			h.ServeHTTP(w, r)
		})
	})

	// Frontend - serve React SPA
	r.Get("/", s.handleFrontend)
	r.Get("/index.html", s.handleFrontend)
	r.Get("/assets/*", s.handleStatic)
	r.Get("/favicon.svg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "frontend/favicon.svg")
	})
	r.Get("/particles.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "frontend/particles.js")
	})
	r.Get("/{*path}", s.handleFrontend) // Catch-all for SPA routes

	// API routes
	r.Route("/api", func(r chi.Router) {
		if s.lastfmScrobbler != nil {
			r.Use(s.sessionMiddleware)
		}

		r.Get("/search", s.handleSearch)
		r.Get("/artist/albums", s.handleArtistAlbums)
		r.Get("/album/tracks", s.handleAlbumTracks)
		r.Get("/queue", s.handleGetQueue)
		r.Post("/queue/add", s.handleAddToQueue)
		r.Post("/queue/add-album", s.handleAddAlbum)
		r.Delete("/queue/{idx}", s.handleRemoveFromQueue)
		r.Post("/queue/clear", s.handleClearQueue)
		r.Post("/queue/reorder", s.handleReorderQueue)
		r.Post("/queue/shuffle", s.handleShuffleQueue)
		r.Get("/state", s.handleGetState)
		r.Post("/play", s.handlePlay)
		r.Post("/pause", s.handlePause)
		r.Post("/stop", s.handleStop)
		r.Post("/next", s.handleNext)
		r.Post("/seek/{idx}", s.handleSeek)
		r.Post("/volume", s.handleVolume)
		r.Get("/sse", s.handleSSE)
		r.Get("/cover/{id}", s.handleCoverArt)
		r.Get("/sync/status", s.handleSyncStatus)
		r.Post("/sync", s.handleSync)
		r.Post("/radio", s.handleRadioConfig)
		r.Get("/genres", s.handleSearchGenres)
		r.Get("/genre/tracks", s.handleGenreTracks)
		r.Get("/upnp/status", s.handleUPnPStatus)
		r.Post("/upnp/reconnect", s.handleUPnPReconnect)

		// Last.fm routes (only if configured)
		if s.lastfmScrobbler != nil {
			r.Get("/me", s.handleMe)
			r.Put("/me/name", s.handleSetName)
			r.Get("/lastfm/link", s.handleLastFMLink)
			r.Get("/lastfm/callback", s.handleLastFMCallback)
			r.Delete("/lastfm/link", s.handleLastFMUnlink)
		}
	})

	return r
}

// handleFrontend serves the React frontend
func (s *Server) handleFrontend(w http.ResponseWriter, r *http.Request) {
	// Serve the static HTML file
	http.ServeFile(w, r, "frontend/index.html")
}

// handleStatic serves static files (JS/CSS assets)
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	path := chi.URLParam(r, "*")
	http.ServeFile(w, r, fmt.Sprintf("frontend/assets/%s", path))
}

// parsePagination reads limit/offset from the query string with sane defaults and caps.
func parsePagination(r *http.Request, defaultLimit, maxLimit int) (limit, offset int) {
	limit = defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return
}

// handleSearch handles track, album, and artist search with pagination.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	searchType := r.URL.Query().Get("type") // "tracks" (default), "albums", "artists"
	limit, offset := parsePagination(r, 100, 500)
	log.Printf("[search] query=%q type=%q limit=%d offset=%d", query, searchType, limit, offset)

	w.Header().Set("Content-Type", "application/json")

	key := "tracks"
	switch searchType {
	case "albums":
		key = "albums"
	case "artists":
		key = "artists"
	}

	if query == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{key: []map[string]interface{}{}, "hasMore": false, "nextOffset": offset})
		return
	}

	var results []map[string]interface{}
	var err error
	switch searchType {
	case "albums":
		results, err = s.lib.SearchAlbums(query, limit, offset)
	case "artists":
		results, err = s.lib.SearchArtists(query, limit, offset)
	default:
		results, err = s.lib.Search(query, limit, offset)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if results == nil {
		results = []map[string]interface{}{}
	}
	hasMore := len(results) == limit
	json.NewEncoder(w).Encode(map[string]interface{}{
		key:          results,
		"hasMore":    hasMore,
		"nextOffset": offset + len(results),
	})
}

// handleArtistAlbums returns albums by a given artist
func (s *Server) handleArtistAlbums(w http.ResponseWriter, r *http.Request) {
	artist := r.URL.Query().Get("artist")
	if artist == "" {
		http.Error(w, "artist required", http.StatusBadRequest)
		return
	}
	results, err := s.lib.GetArtistAlbums(artist)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]map[string]interface{}{"albums": results})
}

// handleAlbumTracks returns tracks for a given album
func (s *Server) handleAlbumTracks(w http.ResponseWriter, r *http.Request) {
	albumID := r.URL.Query().Get("albumId")
	if albumID == "" {
		http.Error(w, "albumId required", http.StatusBadRequest)
		return
	}
	results, err := s.lib.GetAlbumTracks(albumID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]map[string]interface{}{"tracks": results})
}

// handleGetQueue returns the current queue
func (s *Server) handleGetQueue(w http.ResponseWriter, r *http.Request) {
	state := s.queueEngine.State()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

// handleAddToQueue adds a track to the queue
func (s *Server) handleAddToQueue(w http.ResponseWriter, r *http.Request) {
	var item models.QueueItem
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if r.URL.Query().Get("next") == "1" {
		s.queueEngine.InsertNext(item)
	} else {
		s.queueEngine.Add(item)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleAddAlbum adds all tracks from an album to the queue in order
func (s *Server) handleAddAlbum(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AlbumID string `json:"albumId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.AlbumID == "" {
		http.Error(w, "albumId required", http.StatusBadRequest)
		return
	}

	tracks, err := s.lib.GetAlbumTracks(req.AlbumID)
	if err != nil {
		log.Printf("[addAlbum] error fetching tracks: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	insertNext := r.URL.Query().Get("next") == "1"
	if insertNext {
		// Insert in reverse so they stack in correct album order after current track
		for i := len(tracks) - 1; i >= 0; i-- {
			t := tracks[i]
			item := models.QueueItem{
				ID:       t["id"].(string),
				Title:    t["title"].(string),
				Artist:   t["artist"].(string),
				Album:    t["album"].(string),
				Duration: t["duration"].(int),
				CoverArt: t["coverArt"].(string),
			}
			s.queueEngine.InsertNext(item)
		}
	} else {
		for _, t := range tracks {
			item := models.QueueItem{
				ID:       t["id"].(string),
				Title:    t["title"].(string),
				Artist:   t["artist"].(string),
				Album:    t["album"].(string),
				Duration: t["duration"].(int),
				CoverArt: t["coverArt"].(string),
			}
			s.queueEngine.Add(item)
		}
	}

	log.Printf("[addAlbum] queued %d tracks for album %s", len(tracks), req.AlbumID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "tracksAdded": len(tracks)})
}

// handleRemoveFromQueue removes a track from the queue
func (s *Server) handleRemoveFromQueue(w http.ResponseWriter, r *http.Request) {
	idxStr := chi.URLParam(r, "idx")
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		http.Error(w, "Invalid index", http.StatusBadRequest)
		return
	}

	s.queueEngine.Remove(idx)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleClearQueue clears the queue
func (s *Server) handleClearQueue(w http.ResponseWriter, r *http.Request) {
	s.queueEngine.Clear()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleReorderQueue moves a queue item from one position to another
func (s *Server) handleReorderQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From    int    `json:"from"`
		To      int    `json:"to"`
		TrackID string `json:"trackId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !s.queueEngine.Reorder(req.From, req.To, req.TrackID) {
		http.Error(w, "reorder rejected (stale index or track mismatch)", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleShuffleQueue shuffles the queue
func (s *Server) handleShuffleQueue(w http.ResponseWriter, r *http.Request) {
	s.queueEngine.Shuffle()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleGetState returns current state
func (s *Server) handleGetState(w http.ResponseWriter, r *http.Request) {
	state := s.queueEngine.State()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

// handlePlay starts playback
func (s *Server) handlePlay(w http.ResponseWriter, r *http.Request) {
	np := s.queueEngine.NowPlaying()

	if np != nil && s.isTrackLoaded(np) {
		// Track loaded, just resume
		log.Printf("[play] resuming playback")
		s.upnpMu.RLock()
		control := s.upnpControl
		s.upnpMu.RUnlock()
		if control != nil {
			if err := control.Play("1"); err != nil {
				log.Printf("[play] resume error: %v", err)
			}
		}
		s.queueEngine.SetRunning(true)
	} else {
		// Nothing loaded — pop next from queue
		next := s.queueEngine.PopNext()
		if next == nil {
			log.Printf("[play] queue is empty, nothing to play")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{\"ok\":true}"))
			return
		}
		s.queueEngine.SetRunning(true)
		log.Printf("[play] starting: %s - %s", next.Artist, next.Title)
		if err := s.playTrack(next); err != nil {
			log.Printf("[play] error: %v", err)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handlePause pauses playback
func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control != nil {
		control.Pause()
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleStop stops playback, clears the queue, and ends all listening sessions
func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control != nil {
		control.Stop()
	}
	s.queueEngine.Clear()
	s.queueEngine.SetRunning(false)
	if s.lastfmScrobbler != nil {
		s.lastfmScrobbler.OnStop()
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleNext skips to next track
func (s *Server) handleNext(w http.ResponseWriter, r *http.Request) {
	s.playNextInQueue()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleSeek plays a specific track from the queue by index (removes it from queue)
func (s *Server) handleSeek(w http.ResponseWriter, r *http.Request) {
	idxStr := chi.URLParam(r, "idx")
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		http.Error(w, "Invalid index", http.StatusBadRequest)
		return
	}

	// Get the track before removing
	state := s.queueEngine.State()
	if idx < 0 || idx >= len(state.Queue) {
		http.Error(w, "Index out of range", http.StatusBadRequest)
		return
	}
	track := state.Queue[idx]
	s.queueEngine.Remove(idx)

	s.queueEngine.SetRunning(true)
	if err := s.playTrack(&track); err != nil {
		log.Printf("Error playing track: %v", err)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleVolume sets the renderer's master volume (0-100).
func (s *Server) handleVolume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Volume int `json:"volume"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.upnpMu.RLock()
	control := s.upnpControl
	s.upnpMu.RUnlock()
	if control == nil {
		http.Error(w, "upnp not connected", http.StatusServiceUnavailable)
		return
	}

	if err := control.SetVolume("0", req.Volume); err != nil {
		log.Printf("[volume] set failed: %v", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

// handleSSE serves Server-Sent Events for real-time updates
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher, _ := w.(http.Flusher)

	ch := s.queueEngine.Subscribe()
	defer s.queueEngine.Unsubscribe(ch)

	// Mark user as listening if they have a Last.fm link
	if s.lastfmScrobbler != nil {
		if userID := getUserID(r); userID != "" {
			store := s.lastfmScrobbler.Store()
			if _, _, err := store.GetLastFMLink(userID); err == nil {
				store.SetListening(userID, time.Now().Add(12*time.Hour))
			}
		}
	}

	// Send initial state
	state := s.queueEngine.State()
	data, _ := json.Marshal(state)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// handleCoverArt proxies cover art requests
func (s *Server) handleCoverArt(w http.ResponseWriter, r *http.Request) {
	coverArtID := chi.URLParam(r, "id")
	if coverArtID == "" {
		http.Error(w, "No cover art ID", http.StatusBadRequest)
		return
	}

	// Fetch from Navidrome
	url := s.navidrome.CoverArtURL(coverArtID, 300)
	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	io.Copy(w, resp.Body)
}

// handleSyncStatus returns the current sync status
func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	lib := s.GetLibrary()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"isSyncing": lib.IsSyncing(),
		"songCount": lib.GetSongCount(),
	})
}

// handleSync triggers a library sync
func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if s.lib.IsSyncing() {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "sync already in progress"})
		return
	}

	go func() {
		ctx := context.Background()
		if err := s.SyncLibrary(ctx); err != nil {
			log.Printf("Library sync failed: %v", err)
		}
	}()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "sync started"})
}

// handleUPnPStatus returns the current UPnP connection status
func (s *Server) handleUPnPStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": s.GetUPnPStatus(),
	})
}

// handleRadioConfig accepts a full RadioConfig payload, persists it, and (if
// enabled and queue is below threshold) immediately triggers a fill.
func (s *Server) handleRadioConfig(w http.ResponseWriter, r *http.Request) {
	var cfg models.RadioConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.queueEngine.SetRadioConfig(cfg)
	log.Printf("[radio] config: enabled=%v sim-songs=%v sim-artists=%v sim-genres=%v threshold=%d batch=%d",
		cfg.Enabled, cfg.SimilarSongs, cfg.SimilarArtists, cfg.SimilarGenres, cfg.QueueThreshold, cfg.BatchSize)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{\"ok\":true}"))
}

// radioFill picks tracks based on the current RadioConfig and queue contents,
// then appends them to the queue. With no similarity pools enabled, falls back
// to today's behavior (random tracks from the library).
func (s *Server) radioFill() {
	cfg := s.queueEngine.RadioConfig()
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 10
	}

	queueSnap := s.queueEngine.QueueSnapshot()
	recent := s.queueEngine.RecentIDs()
	useSimilarity := cfg.SimilarSongs || cfg.SimilarArtists || cfg.SimilarGenres

	// pickRandomFresh draws up to want random tracks, skipping anything in
	// hardExclude (queue/now-playing dupes) and, on the first pass, anything
	// in softExclude (recency cache). Falls back to softExclude-permitted
	// picks only if the strict pass underdelivered — with 100k+ libraries
	// that fallback should basically never fire.
	pickRandomFresh := func(want int, hardExclude, softExclude map[string]struct{}) []models.QueueItem {
		if want <= 0 {
			return nil
		}
		out := make([]models.QueueItem, 0, want)
		taken := make(map[string]struct{}, want)
		for _, t := range s.lib.GetRandomTracks(want * 4) {
			if _, ok := hardExclude[t.ID]; ok {
				continue
			}
			if _, ok := softExclude[t.ID]; ok {
				continue
			}
			if _, ok := taken[t.ID]; ok {
				continue
			}
			out = append(out, t)
			taken[t.ID] = struct{}{}
			if len(out) >= want {
				return out
			}
		}
		if len(out) < want {
			for _, t := range s.lib.GetRandomTracks(want * 2) {
				if _, ok := hardExclude[t.ID]; ok {
					continue
				}
				if _, ok := taken[t.ID]; ok {
					continue
				}
				out = append(out, t)
				taken[t.ID] = struct{}{}
				if len(out) >= want {
					break
				}
			}
		}
		return out
	}

	queued := make(map[string]struct{}, len(queueSnap))
	for _, q := range queueSnap {
		queued[q.ID] = struct{}{}
	}

	if !useSimilarity || len(queueSnap) == 0 {
		tracks := pickRandomFresh(batch, queued, recent)
		for _, t := range tracks {
			s.queueEngine.Add(t)
		}
		log.Printf("[radio] added %d random tracks (queued=%d recent=%d excluded)", len(tracks), len(queued), len(recent))
		return
	}

	// Bound the seeds so API call count stays predictable regardless of queue length.
	const maxSeeds = 5
	seeds := queueSnap
	if len(seeds) > maxSeeds {
		seeds = seeds[:maxSeeds]
	}

	// `already` excludes both queued tracks (so we don't dupe) and recent
	// tracks (so we don't loop back into the last 6h of listening).
	already := make(map[string]struct{}, len(queueSnap)+len(recent))
	for _, q := range queueSnap {
		already[q.ID] = struct{}{}
	}
	for id := range recent {
		already[id] = struct{}{}
	}

	candidates := make([]models.QueueItem, 0, batch*4)
	addCandidate := func(item models.QueueItem) {
		if item.ID == "" {
			return
		}
		if _, ok := already[item.ID]; ok {
			return
		}
		already[item.ID] = struct{}{}
		candidates = append(candidates, item)
	}

	if cfg.SimilarSongs && s.navidrome != nil {
		for _, seed := range seeds {
			tracks, err := s.navidrome.GetSimilarSongs(seed.ID, 20)
			if err != nil {
				log.Printf("[radio] similar-songs lookup failed for %s: %v", seed.ID, err)
				continue
			}
			for _, t := range tracks {
				addCandidate(searchTrackToQueueItem(t))
			}
		}
	}

	if cfg.SimilarArtists {
		seenArtists := map[string]struct{}{}
		artists := []string{}
		for _, q := range queueSnap {
			if q.Artist == "" {
				continue
			}
			if _, ok := seenArtists[q.Artist]; ok {
				continue
			}
			seenArtists[q.Artist] = struct{}{}
			artists = append(artists, q.Artist)
		}
		for _, t := range s.lib.GetRandomTracksByArtists(artists, batch*3) {
			addCandidate(t)
		}
	}

	if cfg.SimilarGenres {
		ids := make([]string, 0, len(queueSnap))
		for _, q := range queueSnap {
			ids = append(ids, q.ID)
		}
		genres := s.lib.GenresOfTrackIDs(ids)
		for _, t := range s.lib.GetRandomTracksByGenres(genres, batch*3) {
			addCandidate(t)
		}
	}

	if len(candidates) == 0 {
		tracks := pickRandomFresh(batch, queued, recent)
		for _, t := range tracks {
			s.queueEngine.Add(t)
		}
		log.Printf("[radio] no similarity candidates found, added %d random tracks (recent=%d excluded)", len(tracks), len(recent))
		return
	}

	mathrand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	pick := batch
	if pick > len(candidates) {
		pick = len(candidates)
	}
	for _, t := range candidates[:pick] {
		s.queueEngine.Add(t)
	}

	// Top up with random library tracks if similarity didn't yield enough.
	// `already` already covers queued + recent + chosen similar candidates.
	if pick < batch {
		need := batch - pick
		extras := pickRandomFresh(need, already, nil)
		for _, t := range extras {
			already[t.ID] = struct{}{}
			s.queueEngine.Add(t)
		}
		log.Printf("[radio] added %d similar + %d random tracks (recent=%d excluded)", pick, len(extras), len(recent))
		return
	}

	log.Printf("[radio] added %d similar tracks (pools: songs=%v artists=%v genres=%v)",
		pick, cfg.SimilarSongs, cfg.SimilarArtists, cfg.SimilarGenres)
}

// searchTrackToQueueItem converts a Navidrome SearchTrack into a QueueItem.
// Used for results returned by the similarity endpoints — we don't always have
// these tracks indexed locally.
func searchTrackToQueueItem(t models.SearchTrack) models.QueueItem {
	return models.QueueItem{
		ID:       t.ID,
		Title:    t.Title,
		Artist:   t.Artist,
		Album:    t.Album,
		Duration: t.Duration,
		CoverArt: t.CoverArt,
	}
}

// handleSearchGenres returns distinct genres matching the query string, paginated.
func (s *Server) handleSearchGenres(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	limit, offset := parsePagination(r, 100, 500)
	results, err := s.lib.SearchGenres(q, limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if results == nil {
		results = []map[string]interface{}{}
	}
	hasMore := len(results) == limit
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"genres":     results,
		"hasMore":    hasMore,
		"nextOffset": offset + len(results),
	})
}

// handleGenreTracks returns all tracks with the given genre.
func (s *Server) handleGenreTracks(w http.ResponseWriter, r *http.Request) {
	genre := r.URL.Query().Get("genre")
	if genre == "" {
		http.Error(w, "missing genre", http.StatusBadRequest)
		return
	}
	results, err := s.lib.GetTracksByGenre(genre)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if results == nil {
		results = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"tracks": results})
}

// handleUPnPReconnect retries connecting to the renderer
func (s *Server) handleUPnPReconnect(w http.ResponseWriter, r *http.Request) {
	go func() {
		if err := s.ReconnectUPnP(); err != nil {
			log.Printf("UPnP reconnect failed: %v", err)
		} else {
			log.Printf("UPnP reconnect succeeded")
		}
	}()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "reconnect initiated"})
}

// --- Session middleware & Last.fm handlers ---

type contextKey string

const userIDKey contextKey = "userID"

func getUserID(r *http.Request) string {
	if v := r.Context().Value(userIDKey); v != nil {
		return v.(string)
	}
	return ""
}

func (s *Server) sessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		store := s.lastfmScrobbler.Store()

		cookie, err := r.Cookie("jukebox_session")
		if err != nil || cookie.Value == "" {
			// Generate new session
			token := newSessionToken()
			user, err := store.GetOrCreateUser(token)
			if err != nil {
				log.Printf("[session] failed to create user: %v", err)
				next.ServeHTTP(w, r)
				return
			}
			http.SetCookie(w, &http.Cookie{
				Name:     "jukebox_session",
				Value:    token,
				Path:     "/",
				MaxAge:   365 * 24 * 60 * 60,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
			})
			ctx := context.WithValue(r.Context(), userIDKey, user.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		user, err := store.GetOrCreateUser(cookie.Value)
		if err != nil {
			log.Printf("[session] failed to lookup user: %v", err)
			next.ServeHTTP(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), userIDKey, user.ID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newSessionToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	userID := getUserID(r)
	if userID == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"lastfmEnabled": s.lastfmScrobbler != nil,
		})
		return
	}

	store := s.lastfmScrobbler.Store()
	user, err := store.GetUser(userID)
	if err != nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":            user.ID,
		"name":          user.Name,
		"lastfmUser":    nilIfEmpty(user.LastFMUser),
		"isListening":   user.IsListening,
		"lastfmEnabled": true,
	})
}

func (s *Server) handleSetName(w http.ResponseWriter, r *http.Request) {
	userID := getUserID(r)
	if userID == "" {
		http.Error(w, "no session", http.StatusUnauthorized)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	store := s.lastfmScrobbler.Store()
	if err := store.SetUserName(userID, req.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

func (s *Server) handleLastFMLink(w http.ResponseWriter, r *http.Request) {
	// Build callback URL from request
	scheme := "http"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS != nil {
		scheme = "https"
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	callbackURL := fmt.Sprintf("%s://%s/api/lastfm/callback", scheme, host)
	http.Redirect(w, r, s.lastfmScrobbler.AuthURL(callbackURL), http.StatusFound)
}

func (s *Server) handleLastFMCallback(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}

	userID := getUserID(r)
	if userID == "" {
		http.Error(w, "no session", http.StatusUnauthorized)
		return
	}

	sessionKey, lfmUser, err := s.lastfmScrobbler.GetSession(token)
	if err != nil {
		log.Printf("[lastfm] auth failed: %v", err)
		http.Error(w, "Last.fm authorization failed", http.StatusBadGateway)
		return
	}

	store := s.lastfmScrobbler.Store()
	if err := store.LinkLastFM(userID, sessionKey, lfmUser); err != nil {
		log.Printf("[lastfm] link failed: %v", err)
		http.Error(w, "failed to save link", http.StatusInternalServerError)
		return
	}

	log.Printf("[lastfm] linked user %s to Last.fm account %s", userID, lfmUser)

	// Mark as listening immediately
	store.SetListening(userID, time.Now().Add(12*time.Hour))

	http.Redirect(w, r, "/", http.StatusFound)
}

func (s *Server) handleLastFMUnlink(w http.ResponseWriter, r *http.Request) {
	userID := getUserID(r)
	if userID == "" {
		http.Error(w, "no session", http.StatusUnauthorized)
		return
	}

	store := s.lastfmScrobbler.Store()
	if err := store.UnlinkLastFM(userID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"ok\":true}"))
}

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
