package library

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/hecate/navidrome-jukebox/internal/models"
	"github.com/hecate/navidrome-jukebox/internal/navidrome"
	_ "github.com/mattn/go-sqlite3"
)

// Library manages the local song cache and sync
type Library struct {
	db        *sql.DB
	client    *navidrome.Client
	mu        sync.RWMutex
	isSyncing bool
	songCount int
}

// NewLibrary creates a new library manager
func NewLibrary(dbPath string, client *navidrome.Client) (*Library, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS songs (
			id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			artist TEXT NOT NULL,
			album TEXT NOT NULL,
			album_id TEXT DEFAULT '',
			track_number INTEGER DEFAULT 0,
			duration INTEGER DEFAULT 0,
			cover_art TEXT,
			genre TEXT DEFAULT '',
			suffix TEXT DEFAULT '',
			bit_rate INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Migrate: add columns if missing (existing DBs), then create indexes
	db.Exec(`ALTER TABLE songs ADD COLUMN duration INTEGER DEFAULT 0`)
	db.Exec(`ALTER TABLE songs ADD COLUMN album_id TEXT DEFAULT ''`)
	db.Exec(`ALTER TABLE songs ADD COLUMN track_number INTEGER DEFAULT 0`)
	db.Exec(`ALTER TABLE songs ADD COLUMN genre TEXT DEFAULT ''`)
	db.Exec(`ALTER TABLE songs ADD COLUMN suffix TEXT DEFAULT ''`)
	db.Exec(`ALTER TABLE songs ADD COLUMN bit_rate INTEGER DEFAULT 0`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_songs_title ON songs(title)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_songs_album ON songs(album)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_songs_album_id ON songs(album_id)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_songs_genre ON songs(genre)`)

	l := &Library{
		db:     db,
		client: client,
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM songs").Scan(&count)
	if err != nil {
		return nil, fmt.Errorf("failed to count songs: %w", err)
	}
	l.songCount = count

	return l, nil
}

// Sync performs a full library sync from Navidrome.
// onProgress, if non-nil, is called after each fetched page with the running
// total of songs pulled so far — useful for streaming progress to the UI.
func (l *Library) Sync(ctx context.Context, onProgress func(synced int)) error {
	l.mu.Lock()
	if l.isSyncing {
		l.mu.Unlock()
		return nil
	}
	l.isSyncing = true
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		l.isSyncing = false
		l.mu.Unlock()
	}()

	fmt.Println("Starting library sync...")

	const pageSize = 500
	var allSongs []models.SearchTrack
	var songOffset int

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		songs, err := l.client.SearchAll(pageSize, songOffset)
		if err != nil {
			return fmt.Errorf("failed to search: %w", err)
		}

		allSongs = append(allSongs, songs...)
		fmt.Printf("Fetched %d songs (total: %d)\n", len(songs), len(allSongs))

		if onProgress != nil {
			onProgress(len(allSongs))
		}

		if len(songs) < pageSize {
			break
		}
		songOffset += pageSize
	}

	tx, err := l.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM songs")
	if err != nil {
		return fmt.Errorf("failed to clear songs: %w", err)
	}

	insertStmt, err := tx.Prepare("INSERT OR REPLACE INTO songs (id, title, artist, album, album_id, track_number, duration, cover_art, genre, suffix, bit_rate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer insertStmt.Close()

	for _, song := range allSongs {
		_, err := insertStmt.Exec(song.ID, song.Title, song.Artist, song.Album, song.AlbumID, song.Track, song.Duration, song.CoverArt, song.Genre, song.Suffix, song.BitRate)
		if err != nil {
			fmt.Printf("Warning: failed to insert song %s: %v\n", song.Title, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	l.mu.Lock()
	l.songCount = len(allSongs)
	l.mu.Unlock()

	fmt.Printf("Library sync complete: %d songs\n", len(allSongs))
	return nil
}

// Search searches for songs by title with pagination.
func (l *Library) Search(query string, limit, offset int) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, ''),
		       COALESCE(suffix, ''), COALESCE(bit_rate, 0)
		FROM songs
		WHERE title LIKE ?
		ORDER BY
			CASE
				WHEN title LIKE ? THEN 0
				WHEN title LIKE ? THEN 1
				ELSE 2
			END,
			title
		LIMIT ? OFFSET ?
	`, "%"+query+"%", query, query+"%", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, title, artist, album, coverArt, suffix string
		var duration, bitRate int
		if err := rows.Scan(&id, &title, &artist, &album, &duration, &coverArt, &suffix, &bitRate); err != nil {
			log.Printf("[library.Search] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"id":       id,
			"title":    title,
			"artist":   artist,
			"album":    album,
			"duration": duration,
			"coverArt": coverArt,
			"suffix":   suffix,
			"bitRate":  bitRate,
		})
	}

	return results, rows.Err()
}

// SearchAlbums searches for albums by name, returning grouped results, with pagination.
func (l *Library) SearchAlbums(query string, limit, offset int) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT album_id, album, MIN(artist), MIN(COALESCE(cover_art, '')),
		       COUNT(*) as track_count, SUM(COALESCE(duration, 0)) as total_duration
		FROM songs
		WHERE album LIKE ?
		GROUP BY album_id, album
		ORDER BY album
		LIMIT ? OFFSET ?
	`, "%"+query+"%", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to search albums: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var albumID, album, artist, coverArt string
		var trackCount, totalDuration int
		if err := rows.Scan(&albumID, &album, &artist, &coverArt, &trackCount, &totalDuration); err != nil {
			log.Printf("[library.SearchAlbums] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"albumId":       albumID,
			"album":         album,
			"artist":        artist,
			"coverArt":      coverArt,
			"trackCount":    trackCount,
			"totalDuration": totalDuration,
		})
	}

	return results, rows.Err()
}

// SearchArtists searches for distinct artists by name, with pagination.
func (l *Library) SearchArtists(query string, limit, offset int) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT artist, MIN(COALESCE(cover_art, '')), COUNT(DISTINCT album_id) as album_count
		FROM songs
		WHERE artist LIKE ?
		GROUP BY artist
		ORDER BY artist
		LIMIT ? OFFSET ?
	`, "%"+query+"%", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to search artists: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var artist, coverArt string
		var albumCount int
		if err := rows.Scan(&artist, &coverArt, &albumCount); err != nil {
			log.Printf("[library.SearchArtists] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"artist":     artist,
			"coverArt":   coverArt,
			"albumCount": albumCount,
		})
	}

	return results, rows.Err()
}

// GetArtistAlbums returns all albums by a given artist
func (l *Library) GetArtistAlbums(artist string) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT album_id, album, MIN(COALESCE(cover_art, '')),
		       COUNT(*) as track_count, SUM(COALESCE(duration, 0)) as total_duration
		FROM songs
		WHERE artist = ?
		GROUP BY album_id, album
		ORDER BY album
	`, artist)
	if err != nil {
		return nil, fmt.Errorf("failed to get artist albums: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var albumID, album, coverArt string
		var trackCount, totalDuration int
		if err := rows.Scan(&albumID, &album, &coverArt, &trackCount, &totalDuration); err != nil {
			log.Printf("[library.GetArtistAlbums] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"albumId":       albumID,
			"album":         album,
			"artist":        artist,
			"coverArt":      coverArt,
			"trackCount":    trackCount,
			"totalDuration": totalDuration,
		})
	}

	return results, rows.Err()
}

// GetAlbumTracks returns all tracks for an album, ordered by track number
func (l *Library) GetAlbumTracks(albumID string) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, ''), track_number,
		       COALESCE(suffix, ''), COALESCE(bit_rate, 0)
		FROM songs
		WHERE album_id = ?
		ORDER BY track_number, title
	`, albumID)
	if err != nil {
		return nil, fmt.Errorf("failed to get album tracks: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, title, artist, album, coverArt, suffix string
		var duration, trackNumber, bitRate int
		if err := rows.Scan(&id, &title, &artist, &album, &duration, &coverArt, &trackNumber, &suffix, &bitRate); err != nil {
			log.Printf("[library.GetAlbumTracks] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"id":       id,
			"title":    title,
			"artist":   artist,
			"album":    album,
			"duration": duration,
			"coverArt": coverArt,
			"track":    trackNumber,
			"suffix":   suffix,
			"bitRate":  bitRate,
		})
	}

	return results, rows.Err()
}

// GetRandomTracks returns n random tracks from the library
func (l *Library) GetRandomTracks(n int) []models.QueueItem {
	rows, err := l.db.Query(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, '')
		FROM songs ORDER BY RANDOM() LIMIT ?
	`, n)
	if err != nil {
		log.Printf("[library.GetRandomTracks] query error: %v", err)
		return nil
	}
	defer rows.Close()

	var tracks []models.QueueItem
	for rows.Next() {
		var item models.QueueItem
		if err := rows.Scan(&item.ID, &item.Title, &item.Artist, &item.Album, &item.Duration, &item.CoverArt); err != nil {
			continue
		}
		tracks = append(tracks, item)
	}
	return tracks
}

// GetTrackByID looks up a single track by its Navidrome ID
func (l *Library) GetTrackByID(trackID string) *models.QueueItem {
	var id, title, artist, album, coverArt string
	var duration int
	err := l.db.QueryRow(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, '')
		FROM songs WHERE id = ?
	`, trackID).Scan(&id, &title, &artist, &album, &duration, &coverArt)
	if err != nil {
		return nil
	}
	return &models.QueueItem{
		ID:       id,
		Title:    title,
		Artist:   artist,
		Album:    album,
		Duration: duration,
		CoverArt: coverArt,
	}
}

func (l *Library) GetSongCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.songCount
}

func (l *Library) IsSyncing() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isSyncing
}

// GetRandomTracksByArtists returns up to n random tracks where artist is in the
// provided set. Returns empty slice if artists is empty.
func (l *Library) GetRandomTracksByArtists(artists []string, n int) []models.QueueItem {
	if len(artists) == 0 || n <= 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(artists))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, '')
		FROM songs WHERE artist IN (%s) ORDER BY RANDOM() LIMIT ?
	`, placeholders)
	args := make([]interface{}, 0, len(artists)+1)
	for _, a := range artists {
		args = append(args, a)
	}
	args = append(args, n)
	rows, err := l.db.Query(query, args...)
	if err != nil {
		log.Printf("[library.GetRandomTracksByArtists] query error: %v", err)
		return nil
	}
	defer rows.Close()
	var tracks []models.QueueItem
	for rows.Next() {
		var t models.QueueItem
		if err := rows.Scan(&t.ID, &t.Title, &t.Artist, &t.Album, &t.Duration, &t.CoverArt); err != nil {
			continue
		}
		tracks = append(tracks, t)
	}
	return tracks
}

// GetRandomTracksByGenres returns up to n random tracks where genre is in the
// provided set. Empty/blank genres in the input are ignored.
func (l *Library) GetRandomTracksByGenres(genres []string, n int) []models.QueueItem {
	clean := make([]string, 0, len(genres))
	for _, g := range genres {
		if strings.TrimSpace(g) != "" {
			clean = append(clean, g)
		}
	}
	if len(clean) == 0 || n <= 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(clean))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, '')
		FROM songs WHERE genre IN (%s) ORDER BY RANDOM() LIMIT ?
	`, placeholders)
	args := make([]interface{}, 0, len(clean)+1)
	for _, g := range clean {
		args = append(args, g)
	}
	args = append(args, n)
	rows, err := l.db.Query(query, args...)
	if err != nil {
		log.Printf("[library.GetRandomTracksByGenres] query error: %v", err)
		return nil
	}
	defer rows.Close()
	var tracks []models.QueueItem
	for rows.Next() {
		var t models.QueueItem
		if err := rows.Scan(&t.ID, &t.Title, &t.Artist, &t.Album, &t.Duration, &t.CoverArt); err != nil {
			continue
		}
		tracks = append(tracks, t)
	}
	return tracks
}

// GenresOfTrackIDs returns the distinct, non-empty set of genres that appear
// across the supplied track IDs.
func (l *Library) GenresOfTrackIDs(trackIDs []string) []string {
	if len(trackIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(trackIDs))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf(`SELECT DISTINCT genre FROM songs WHERE id IN (%s) AND genre != ''`, placeholders)
	args := make([]interface{}, 0, len(trackIDs))
	for _, id := range trackIDs {
		args = append(args, id)
	}
	rows, err := l.db.Query(query, args...)
	if err != nil {
		log.Printf("[library.GenresOfTrackIDs] query error: %v", err)
		return nil
	}
	defer rows.Close()
	var genres []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			continue
		}
		genres = append(genres, g)
	}
	return genres
}

// SearchGenres returns distinct genres matching the query, with track counts, paginated.
func (l *Library) SearchGenres(query string, limit, offset int) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT genre, COUNT(*) as track_count
		FROM songs
		WHERE genre != '' AND LOWER(genre) LIKE LOWER(?)
		GROUP BY genre
		ORDER BY track_count DESC, genre
		LIMIT ? OFFSET ?
	`, "%"+query+"%", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to search genres: %w", err)
	}
	defer rows.Close()
	var results []map[string]interface{}
	for rows.Next() {
		var genre string
		var trackCount int
		if err := rows.Scan(&genre, &trackCount); err != nil {
			log.Printf("[library.SearchGenres] scan error: %v", err)
			continue
		}
		results = append(results, map[string]interface{}{
			"genre":      genre,
			"trackCount": trackCount,
		})
	}
	return results, rows.Err()
}

// GetTracksByGenre returns all tracks for a given genre (case-insensitive exact match).
func (l *Library) GetTracksByGenre(genre string) ([]map[string]interface{}, error) {
	rows, err := l.db.Query(`
		SELECT id, title, artist, album, COALESCE(duration, 0), COALESCE(cover_art, ''), track_number
		FROM songs
		WHERE LOWER(genre) = LOWER(?)
		ORDER BY artist, album, track_number
		LIMIT 500
	`, genre)
	if err != nil {
		return nil, fmt.Errorf("failed to get tracks by genre: %w", err)
	}
	defer rows.Close()
	var results []map[string]interface{}
	for rows.Next() {
		var id, title, artist, album, coverArt string
		var duration, trackNumber int
		if err := rows.Scan(&id, &title, &artist, &album, &duration, &coverArt, &trackNumber); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"id":       id,
			"title":    title,
			"artist":   artist,
			"album":    album,
			"duration": duration,
			"coverArt": coverArt,
			"track":    trackNumber,
		})
	}
	return results, rows.Err()
}

func (l *Library) Close() error {
	return l.db.Close()
}
