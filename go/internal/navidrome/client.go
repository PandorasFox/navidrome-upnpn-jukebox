package navidrome

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/hecate/navidrome-jukebox/internal/models"
)

// Client handles communication with Navidrome
type Client struct {
	baseURL         string // used for our own API calls (can use hostname)
	rendererBaseURL string // used for URLs sent to UPnP renderer (resolved to IP)
	username        string
	password        string
}

// NewClient creates a new Navidrome client.
// Resolves the hostname in baseURL to an IP for renderer-facing URLs,
// since UPnP renderers typically can't resolve local DNS names.
func NewClient(baseURL, username, password string) *Client {
	rendererBaseURL := baseURL
	if u, err := url.Parse(baseURL); err == nil {
		host := u.Hostname()
		if ip := net.ParseIP(host); ip == nil {
			// It's a hostname, resolve it
			if addrs, err := net.LookupHost(host); err == nil && len(addrs) > 0 {
				port := u.Port()
				if port != "" {
					u.Host = net.JoinHostPort(addrs[0], port)
				} else {
					u.Host = addrs[0]
				}
				rendererBaseURL = u.String()
				log.Printf("[navidrome] resolved %s -> %s for renderer URLs", host, addrs[0])
			}
		}
	}

	return &Client{
		baseURL:         baseURL,
		rendererBaseURL: rendererBaseURL,
		username:        username,
		password:        password,
	}
}

// Search searches for tracks matching the query
func (c *Client) Search(query string) ([]models.SearchTrack, error) {
	// Navidrome Subsonic API: /rest/{action}?params
	// Note: Subsonic API doesn't support field-specific filtering, so we
	// fetch results and filter by title on the client side
	// We also need to paginate through all results

	const pageSize = 500         // Larger pages to reduce requests
	const maxTotalResults = 5000 // Cap to avoid fetching entire library
	var allSongs []models.SearchTrack
	var songOffset int

	for {
		params := url.Values{}
		params.Set("v", "1.16.1")
		params.Set("c", "navidrome-jukebox")
		params.Set("u", c.username)
		params.Set("p", c.password)
		params.Set("q", query)
		params.Set("artistCount", "0")
		params.Set("albumCount", "0")
		params.Set("songCount", fmt.Sprintf("%d", pageSize))
		params.Set("songOffset", fmt.Sprintf("%d", songOffset))

		reqURL := fmt.Sprintf("%s/rest/search3?%s", c.baseURL, params.Encode())

		resp, err := http.Get(reqURL)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		var result models.SearchResponse
		if err := xml.Unmarshal(body, &result); err != nil {
			// Debug: log what we got
			truncated := string(body)
			if len(truncated) > 200 {
				truncated = truncated[:200] + "..."
			}
			return nil, fmt.Errorf("failed to parse response (got: %s): %w", truncated, err)
		}

		allSongs = append(allSongs, result.SearchResult.Song...)

		// Stop if we got fewer results than requested (last page)
		// or if we've reached our maximum
		if len(result.SearchResult.Song) < pageSize || len(allSongs) >= maxTotalResults {
			break
		}
		songOffset += pageSize
	}

	// Filter to only tracks where title contains the query (case-insensitive)
	// Subsonic API doesn't support field-specific filtering
	queryLower := strings.ToLower(query)
	var filtered []models.SearchTrack
	for _, song := range allSongs {
		if strings.Contains(strings.ToLower(song.Title), queryLower) {
			filtered = append(filtered, song)
		}
	}

	return filtered, nil
}

// StreamURL generates a stream URL for a track.
// Uses the resolved IP base URL so UPnP renderers can reach it.
func (c *Client) StreamURL(trackID string) string {
	params := url.Values{}
	params.Set("v", "1.16.1")
	params.Set("c", "navidrome-jukebox")
	params.Set("u", c.username)
	params.Set("p", c.password)
	params.Set("id", trackID)
	// format=raw → no transcoding; the renderer gets the original file
	// (FLAC stays FLAC, MP3 stays MP3, etc.). Yamaha sniffs Content-Type
	// from Navidrome's HTTP response and handles PCM/FLAC natively.
	params.Set("format", "raw")

	return fmt.Sprintf("%s/rest/stream?%s", c.rendererBaseURL, params.Encode())
}

// CoverArtURL generates a cover art URL.
// Uses the resolved IP base URL so UPnP renderers can reach it.
func (c *Client) CoverArtURL(coverArtID string, size int) string {
	params := url.Values{}
	params.Set("v", "1.16.1")
	params.Set("c", "navidrome-jukebox")
	params.Set("u", c.username)
	params.Set("p", c.password)
	params.Set("t", "getCoverArt")
	params.Set("id", coverArtID)
	params.Set("size", fmt.Sprintf("%d", size))

	return fmt.Sprintf("%s/rest/getCoverArt?%s", c.rendererBaseURL, params.Encode())
}

// GetSimilarSongs fetches tracks similar to the given track via /rest/getSimilarSongs.
// On Navidrome with the AudioMuse-AI plugin installed, this is enriched with
// AI-derived similarity beyond the default Last.fm-style suggestions.
func (c *Client) GetSimilarSongs(trackID string, count int) ([]models.SearchTrack, error) {
	params := url.Values{}
	params.Set("v", "1.16.1")
	params.Set("c", "navidrome-jukebox")
	params.Set("u", c.username)
	params.Set("p", c.password)
	params.Set("id", trackID)
	params.Set("count", fmt.Sprintf("%d", count))

	reqURL := fmt.Sprintf("%s/rest/getSimilarSongs?%s", c.baseURL, params.Encode())

	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var result models.SimilarSongsResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return result.SimilarSongs.Song, nil
}

// GetRandomSongsByGenre fetches up to count random songs filtered to the given genre
// via /rest/getRandomSongs.
func (c *Client) GetRandomSongsByGenre(genre string, count int) ([]models.SearchTrack, error) {
	params := url.Values{}
	params.Set("v", "1.16.1")
	params.Set("c", "navidrome-jukebox")
	params.Set("u", c.username)
	params.Set("p", c.password)
	params.Set("genre", genre)
	params.Set("size", fmt.Sprintf("%d", count))

	reqURL := fmt.Sprintf("%s/rest/getRandomSongs?%s", c.baseURL, params.Encode())

	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var result models.RandomSongsResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return result.RandomSongs.Song, nil
}

// SearchAll fetches all songs from the library (for full sync)
// Uses search3 with empty query per Subsonic spec for offline sync
func (c *Client) SearchAll(limit, offset int) ([]models.SearchTrack, error) {
	params := url.Values{}
	params.Set("v", "1.16.1")
	params.Set("c", "navidrome-jukebox")
	params.Set("u", c.username)
	params.Set("p", c.password)
	params.Set("q", "") // Empty query returns all
	params.Set("artistCount", "0")
	params.Set("albumCount", "0")
	params.Set("songCount", fmt.Sprintf("%d", limit))
	params.Set("songOffset", fmt.Sprintf("%d", offset))

	reqURL := fmt.Sprintf("%s/rest/search3?%s", c.baseURL, params.Encode())

	resp, err := http.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var result models.SearchResponse
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result.SearchResult.Song, nil
}
