package models

import (
	"time"
)

// QueueItem represents a track in the playback queue
type QueueItem struct {
	ID       string    `json:"id"`             // Navidrome track ID
	Title    string    `json:"title"`          // Track title
	Artist   string    `json:"artist"`         // Artist name
	Album    string    `json:"album"`          // Album name
	Year     int       `json:"year,omitempty"` // Release year
	Duration int       `json:"duration"`       // Duration in seconds
	CoverArt string    `json:"coverArt"`       // Cover art ID for URL generation
	Suffix   string    `json:"suffix"`         // Codec suffix (flac, mp3, …) — drives DIDL MIME
	BitRate  int       `json:"bitRate"`        // Source bitrate in kbps — surfaced in DIDL <res>
	AddedAt  time.Time `json:"addedAt"`        // When added to queue
	AddedBy  string    `json:"addedBy"`        // "system" or username (we'll skip auth for now)
}

// PlaybackState represents the current state of the UPnP renderer
type PlaybackState struct {
	CurrentURI     string `json:"currentUri"`     // Currently playing URI
	Position       int    `json:"position"`       // Current position in seconds
	Duration       int    `json:"duration"`       // Track duration in seconds
	TransportState string `json:"transportState"` // PLAYING, PAUSED_PLAYBACK, STOPPED, TRANSITIONING
	Volume         int    `json:"volume"`         // 0-100
}

// SystemState represents the full application state for SSE broadcasts
type SystemState struct {
	Queue        []QueueItem   `json:"queue"`
	NowPlaying   *QueueItem    `json:"nowPlaying"`
	Renderer     PlaybackState `json:"renderer"`
	IsRunning    bool          `json:"isRunning"`
	RadioMode    bool          `json:"radioMode"` // mirror of Radio.Enabled for back-compat
	Radio        RadioConfig   `json:"radio"`
	RadioFilling bool          `json:"radioFilling"`
	Sync         SyncState     `json:"sync"`
	UPnPStatus   string        `json:"upnpStatus"`
}

// RadioConfig controls the auto-fill behavior of radio mode
type RadioConfig struct {
	Enabled        bool `json:"enabled"`
	SimilarSongs   bool `json:"similarSongs"`
	SimilarArtists bool `json:"similarArtists"`
	SimilarGenres  bool `json:"similarGenres"`
	QueueThreshold int  `json:"queueThreshold"`
	BatchSize      int  `json:"batchSize"`
}

// SyncState reports library sync progress
type SyncState struct {
	InProgress bool `json:"inProgress"`
	SongCount  int  `json:"songCount"` // total in local DB; shown when idle
	Synced     int  `json:"synced"`    // running tally during a sync
}

// SearchResponse matches Navidrome's Subsonic XML search results
type SearchResponse struct {
	SearchResult struct {
		Artist []SearchArtist `xml:"artist"`
		Album  []SearchAlbum  `xml:"album"`
		Song   []SearchTrack  `xml:"song"`
	} `xml:"searchResult3"`
}

type SearchArtist struct {
	ID    string `xml:"id,attr"`
	Name  string `xml:"name,attr"`
	Cover string `xml:"coverArt,attr"`
}

type SearchAlbum struct {
	ID     string `xml:"id,attr"`
	Artist string `xml:"artist,attr"`
	Title  string `xml:"title,attr"`
	Cover  string `xml:"coverArt,attr"`
	Year   int    `xml:"year,attr"`
}

type SearchTrack struct {
	ID       string `xml:"id,attr" json:"id"`
	Title    string `xml:"title,attr" json:"title"`
	Artist   string `xml:"artist,attr" json:"artist"`
	Album    string `xml:"album,attr" json:"album"`
	AlbumID  string `xml:"albumId,attr" json:"albumId"`
	Track    int    `xml:"track,attr" json:"track"`
	Duration int    `xml:"duration,attr" json:"duration"`
	CoverArt string `xml:"coverArt,attr" json:"coverArt"`
	Genre    string `xml:"genre,attr" json:"genre"`
	Suffix   string `xml:"suffix,attr" json:"suffix"`
	BitRate  int    `xml:"bitRate,attr" json:"bitRate"`
}

// SimilarSongsResponse matches /rest/getSimilarSongs
type SimilarSongsResponse struct {
	SimilarSongs struct {
		Song []SearchTrack `xml:"song"`
	} `xml:"similarSongs"`
}

// RandomSongsResponse matches /rest/getRandomSongs
type RandomSongsResponse struct {
	RandomSongs struct {
		Song []SearchTrack `xml:"song"`
	} `xml:"randomSongs"`
}
