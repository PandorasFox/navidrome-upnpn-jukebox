import { useState, useEffect, useRef } from 'react';
import './App.css';

const API = '/api';

function App() {
  const [queue, setQueue] = useState([]);
  const [nowPlaying, setNowPlaying] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [renderer, setRenderer] = useState({ position: 0, duration: 0, transportState: '', volume: 0 });
  const [volumeDraft, setVolumeDraft] = useState(null); // local override while user is dragging
  const volumeTimerRef = useRef(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchMode, setSearchMode] = useState('tracks');
  const [searchResults, setSearchResults] = useState([]);
  const [addNext, setAddNext] = useState(false);
  const [radioCfg, setRadioCfg] = useState({
    enabled: false,
    similarSongs: false,
    similarArtists: false,
    similarGenres: false,
    queueThreshold: 5,
    batchSize: 10,
  });
  const [syncState, setSyncState] = useState({ inProgress: false, songCount: 0, synced: 0 });
  const [radioFilling, setRadioFilling] = useState(false);
  const [upnpStatus, setUpnpStatus] = useState('');
  const [user, setUser] = useState(null);
  const prevPositionRef = useRef(0);
  const [drillDown, setDrillDown] = useState(null);
  // drillDown: null | { type: 'albumTracks', album, tracks } | { type: 'artistAlbums', artist, albums }
  const searchTimeoutRef = useRef(null);
  const searchContainerRef = useRef(null);

  useEffect(() => {
    const handleClick = (e) => {
      if (searchContainerRef.current && !searchContainerRef.current.contains(e.target)) {
        setSearchResults([]);
        setDrillDown(null);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  useEffect(() => {
    const evtSource = new EventSource(`${API}/sse`);
    evtSource.onmessage = (e) => {
      const state = JSON.parse(e.data);
      setQueue(state.queue || []);
      setNowPlaying(state.nowPlaying || null);
      setIsRunning(state.isRunning ?? false);
      if (state.radio) setRadioCfg(state.radio);
      if (state.sync) setSyncState(state.sync);
      setRadioFilling(state.radioFilling ?? false);
      if (state.upnpStatus) setUpnpStatus(state.upnpStatus);
      if (state.renderer) setRenderer(state.renderer);
    };
    return () => evtSource.close();
  }, []);

  useEffect(() => {
    fetch(`${API}/me`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) setUser(data); })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchResults([]);
      return;
    }
    setDrillDown(null);

    if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    let cancelled = false;
    const PAGE_SIZE = 100;
    const resultKey =
      searchMode === 'albums' ? 'albums' :
      searchMode === 'artists' ? 'artists' :
      searchMode === 'genres' ? 'genres' : 'tracks';
    const url = searchMode === 'genres'
      ? (offset) => `${API}/genres?q=${encodeURIComponent(searchQuery)}&limit=${PAGE_SIZE}&offset=${offset}`
      : (offset) => `${API}/search?q=${encodeURIComponent(searchQuery)}&type=${searchMode}&limit=${PAGE_SIZE}&offset=${offset}`;

    searchTimeoutRef.current = setTimeout(async () => {
      try {
        let offset = 0;
        let firstPage = true;
        while (!cancelled) {
          const res = await fetch(url(offset));
          if (cancelled) return;
          const data = await res.json();
          const page = data[resultKey] || [];
          if (firstPage) {
            setSearchResults(page);
            firstPage = false;
          } else if (page.length > 0) {
            setSearchResults((prev) => [...prev, ...page]);
          }
          if (!data.hasMore || page.length === 0) break;
          offset = data.nextOffset ?? offset + page.length;
        }
      } catch (err) {
        if (!cancelled) console.error('Search failed:', err);
      }
    }, 300);

    return () => {
      cancelled = true;
      if (searchTimeoutRef.current) clearTimeout(searchTimeoutRef.current);
    };
  }, [searchQuery, searchMode]);

  const addTrack = async (track) => {
    await fetch(`${API}/queue/add${addNext ? '?next=1' : ''}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(track),
    });
  };

  const addAlbum = async (album) => {
    await fetch(`${API}/queue/add-album${addNext ? '?next=1' : ''}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ albumId: album.albumId }),
    });
  };

  const drillIntoAlbum = async (album, parentDrill = null) => {
    try {
      const res = await fetch(`${API}/album/tracks?albumId=${encodeURIComponent(album.albumId)}`);
      const data = await res.json();
      setDrillDown({ type: 'albumTracks', album, tracks: data.tracks || [], parentDrill });
    } catch (err) {
      console.error('Failed to load album tracks:', err);
    }
  };

  const drillIntoArtist = async (artist) => {
    try {
      const res = await fetch(`${API}/artist/albums?artist=${encodeURIComponent(artist.artist)}`);
      const data = await res.json();
      setDrillDown({ type: 'artistAlbums', artist: artist.artist, albums: data.albums || [] });
    } catch (err) {
      console.error('Failed to load artist albums:', err);
    }
  };

  const drillIntoGenre = async (genre) => {
    try {
      const res = await fetch(`${API}/genre/tracks?genre=${encodeURIComponent(genre.genre)}`);
      const data = await res.json();
      setDrillDown({ type: 'genreTracks', genre: genre.genre, tracks: data.tracks || [] });
    } catch (err) {
      console.error('Failed to load genre tracks:', err);
    }
  };

  const dismissSearch = () => {
    setSearchQuery('');
    setSearchResults([]);
    setDrillDown(null);
  };

  const removeTrack = async (idx) => {
    await fetch(`${API}/queue/${idx}`, { method: 'DELETE' });
  };

  const dragItem = useRef(null);
  const dragOverItem = useRef(null);

  const handleDragStart = (idx, trackId) => {
    dragItem.current = { idx, trackId };
  };

  const handleDragOver = (e, idx) => {
    e.preventDefault();
    dragOverItem.current = idx;
  };

  const handleDrop = async (e) => {
    e.preventDefault();
    if (dragItem.current === null || dragOverItem.current === null) return;
    const { idx: from, trackId } = dragItem.current;
    const to = dragOverItem.current;
    if (from === to) return;
    await fetch(`${API}/queue/reorder`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ from, to, trackId }),
    });
    dragItem.current = null;
    dragOverItem.current = null;
  };

  const radioPostTimer = useRef(null);
  const postRadioCfg = (cfg) => {
    if (radioPostTimer.current) clearTimeout(radioPostTimer.current);
    radioPostTimer.current = setTimeout(() => {
      fetch(`${API}/radio`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      }).catch(() => {});
    }, 250);
  };
  const updateRadio = (patch) => {
    setRadioCfg((prev) => {
      const next = { ...prev, ...patch };
      postRadioCfg(next);
      return next;
    });
  };

  const triggerSync = () => fetch(`${API}/sync`, { method: 'POST' });

  const reconnectUpnp = () => fetch(`${API}/upnp/reconnect`, { method: 'POST' });

  const play = () => fetch(`${API}/play`, { method: 'POST' });
  const pause = () => fetch(`${API}/pause`, { method: 'POST' });
  const stop = () => fetch(`${API}/stop`, { method: 'POST' });
  const next = () => fetch(`${API}/next`, { method: 'POST' });
  const clearQueue = () => fetch(`${API}/queue/clear`, { method: 'POST' });

  const onVolumeInput = (e) => {
    const v = parseInt(e.target.value, 10);
    setVolumeDraft(v);
    if (volumeTimerRef.current) clearTimeout(volumeTimerRef.current);
    volumeTimerRef.current = setTimeout(() => {
      fetch(`${API}/volume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ volume: v }),
      }).catch(() => {}).finally(() => {
        // Release local override after a short delay so the next SSE tick
        // (which may still report the pre-change value) doesn't snap back.
        setTimeout(() => setVolumeDraft(null), 800);
      });
    }, 150);
  };

  const volumeValue = volumeDraft != null ? volumeDraft : (renderer.volume ?? 0);

  const formatDuration = (seconds) => {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}:${s.toString().padStart(2, '0')}`;
  };

  const currentTrack = nowPlaying;
  const showResults = searchResults.length > 0 || drillDown;
  const progressReset = renderer.position < prevPositionRef.current - 2;
  prevPositionRef.current = renderer.position;

  // --- Render helpers ---

  const renderTrackList = (tracks, { showAlbum = true } = {}) => (
    tracks.map((track) => (
      <div key={track.id} className="result-item" onClick={() => addTrack(track)}>
        <img src={`${API}/cover/${track.coverArt}`} alt="" className="result-art"
          loading="lazy" decoding="async"
          onLoad={(e) => (e.target.style.display = '')}
          onError={(e) => (e.target.style.display = 'none')} />
        <div className="result-info">
          <div className="result-title">{track.title}</div>
          <div className="result-artist">
            {showAlbum ? `${track.artist} — ${track.album}` : track.artist}
          </div>
        </div>
        {track.duration > 0 && <div className="result-duration">{formatDuration(track.duration)}</div>}
        <button className="add-btn">+</button>
      </div>
    ))
  );

  const renderAlbumGrid = (albums, onAlbumClick) => (
    <div className="tile-grid">
      {albums.map((album) => (
        <div key={album.albumId} className="tile" onClick={() => onAlbumClick(album)}>
          <img src={`${API}/cover/${album.coverArt}`} alt="" className="tile-art"
            loading="lazy" decoding="async"
            onLoad={(e) => (e.target.style.visibility = '')}
            onError={(e) => (e.target.style.visibility = 'hidden')} />
          <div className="tile-label">{album.album}</div>
          <div className="tile-sub">{album.artist} · {album.trackCount} tracks</div>
        </div>
      ))}
    </div>
  );

  const renderArtistGrid = (artists) => (
    <div className="tile-grid">
      {artists.map((artist) => (
        <div key={artist.artist} className="tile" onClick={() => drillIntoArtist(artist)}>
          <img src={`${API}/cover/${artist.coverArt}`} alt="" className="tile-art"
            loading="lazy" decoding="async"
            onLoad={(e) => (e.target.style.visibility = '')}
            onError={(e) => (e.target.style.visibility = 'hidden')} />
          <div className="tile-label">{artist.artist}</div>
          <div className="tile-sub">{artist.albumCount} {artist.albumCount === 1 ? 'album' : 'albums'}</div>
        </div>
      ))}
    </div>
  );

  const renderGenreList = (genres) => (
    genres.map((g) => (
      <div key={g.genre} className="result-item" onClick={() => drillIntoGenre(g)}>
        <div className="result-info">
          <div className="result-title">{g.genre}</div>
          <div className="result-artist">{g.trackCount} {g.trackCount === 1 ? 'track' : 'tracks'}</div>
        </div>
      </div>
    ))
  );

  const renderDrillDown = () => {
    if (!drillDown) return null;

    if (drillDown.type === 'albumTracks') {
      const { album, tracks } = drillDown;
      return (
        <>
          <div className="drill-header">
            <button className="back-btn" onClick={() => {
              // Go back: if we came from an artist, restore artist albums; otherwise clear
              setDrillDown(drillDown.parentDrill || null);
            }}>←</button>
            <img src={`${API}/cover/${album.coverArt}`} alt="" className="drill-art"
              decoding="async"
              onLoad={(e) => (e.target.style.display = '')}
              onError={(e) => (e.target.style.display = 'none')} />
            <div className="drill-info">
              <div className="drill-title">{album.album}</div>
              <div className="drill-sub">{album.artist}</div>
            </div>
            <button className="queue-all-btn" onClick={() => { addAlbum(album); dismissSearch(); }}>
              Queue All
            </button>
          </div>
          {renderTrackList(tracks, { showAlbum: false })}
        </>
      );
    }

    if (drillDown.type === 'artistAlbums') {
      const { artist, albums } = drillDown;
      return (
        <>
          <div className="drill-header">
            <button className="back-btn" onClick={() => setDrillDown(null)}>←</button>
            <div className="drill-info">
              <div className="drill-title">{artist}</div>
              <div className="drill-sub">{albums.length} {albums.length === 1 ? 'album' : 'albums'}</div>
            </div>
          </div>
          {renderAlbumGrid(albums, (album) => drillIntoAlbum(album, drillDown))}
        </>
      );
    }

    if (drillDown.type === 'genreTracks') {
      const { genre, tracks } = drillDown;
      return (
        <>
          <div className="drill-header">
            <button className="back-btn" onClick={() => setDrillDown(null)}>←</button>
            <div className="drill-info">
              <div className="drill-title">{genre}</div>
              <div className="drill-sub">{tracks.length} {tracks.length === 1 ? 'track' : 'tracks'}</div>
            </div>
          </div>
          {renderTrackList(tracks)}
        </>
      );
    }

    return null;
  };

  const renderSearchResults = () => {
    if (drillDown) return renderDrillDown();
    if (searchResults.length === 0) return null;

    if (searchMode === 'tracks') return renderTrackList(searchResults);
    if (searchMode === 'albums') return renderAlbumGrid(searchResults, drillIntoAlbum);
    if (searchMode === 'artists') return renderArtistGrid(searchResults);
    if (searchMode === 'genres') return renderGenreList(searchResults);
    return null;
  };

  return (
    <div className="app">
      <header className="header">
        <h1>navidrome upnp jukebox</h1>
      </header>

      <div className="status-bar">
        <div className="status-cell">
          <span className={`upnp-dot ${upnpStatus}`} />
          <span className="status-label">
            {upnpStatus === 'connected' ? 'connected' :
             upnpStatus === 'connecting' ? 'connecting...' : 'disconnected'}
          </span>
          <button className="status-btn" onClick={reconnectUpnp}
            disabled={upnpStatus === 'connecting'}>
            {upnpStatus === 'connected' ? 'reconnect' : 'connect'}
          </button>
        </div>

        <div className="status-cell">
          <span className={`sync-dot ${syncState.inProgress ? 'syncing' : 'idle'}`} />
          <span className="status-label">
            {syncState.inProgress
              ? `syncing… ${syncState.synced.toLocaleString()}`
              : `${syncState.songCount.toLocaleString()} songs`}
          </span>
          <button className="status-btn" onClick={triggerSync} disabled={syncState.inProgress}>
            {syncState.inProgress ? '…' : 're-sync'}
          </button>
        </div>

        {user?.lastfmEnabled && (
          <div className="status-cell">
            {user.lastfmUser ? (
              <>
                <span className="lastfm-dot" />
                <span className="status-label">{user.lastfmUser}</span>
                <button className="status-btn" onClick={async () => {
                  await fetch(`${API}/lastfm/link`, { method: 'DELETE' });
                  setUser(u => ({ ...u, lastfmUser: null }));
                }}>unlink</button>
              </>
            ) : (
              <a href={`${API}/lastfm/link`} className="lastfm-link-btn">
                Link Last.fm
              </a>
            )}
          </div>
        )}
      </div>

      <div className="search-container" ref={searchContainerRef}>
        <input
          type="text"
          className="search-input"
          placeholder={
            searchMode === 'albums' ? 'Search for albums...' :
            searchMode === 'artists' ? 'Search for artists...' :
            searchMode === 'genres' ? 'Search for genres...' :
            'Search for tracks...'
          }
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          autoFocus
        />
        <div className="search-options">
          <div className="search-chips">
            <button className={`chip ${searchMode === 'tracks' ? 'active' : ''}`}
              onClick={() => { setSearchMode('tracks'); setSearchResults([]); setDrillDown(null); }}>
              Tracks
            </button>
            <button className={`chip ${searchMode === 'albums' ? 'active' : ''}`}
              onClick={() => { setSearchMode('albums'); setSearchResults([]); setDrillDown(null); }}>
              Albums
            </button>
            <button className={`chip ${searchMode === 'artists' ? 'active' : ''}`}
              onClick={() => { setSearchMode('artists'); setSearchResults([]); setDrillDown(null); }}>
              Artists
            </button>
            <button className={`chip ${searchMode === 'genres' ? 'active' : ''}`}
              onClick={() => { setSearchMode('genres'); setSearchResults([]); setDrillDown(null); }}>
              Genres
            </button>
          </div>
          <label className="add-next-toggle" onClick={() => setAddNext(!addNext)}>
            <span className="add-next-label">add next?</span>
            <span className={`toggle-switch ${addNext ? 'on' : ''}`}>
              <span className="toggle-knob" />
            </span>
          </label>
        </div>

        {showResults && (
          <div className="search-results">
            {renderSearchResults()}
          </div>
        )}
      </div>

      {currentTrack && (
        <div className="now-playing">
          <img
            src={`${API}/cover/${currentTrack.coverArt}`}
            alt=""
            className="now-playing-art"
            decoding="async"
            onLoad={(e) => (e.target.style.display = '')}
            onError={(e) => (e.target.style.display = 'none')}
          />
          <div className="now-playing-info">
            <div className="now-playing-title">Now Playing: {currentTrack.title}</div>
            <div className="now-playing-artist">{currentTrack.artist}</div>
            {renderer.duration > 0 && (
              <div className="now-playing-progress">
                <div className="progress-bar">
                  <div
                    className={`progress-fill${progressReset ? ' reset' : ''}`}
                    style={{ width: `${Math.min(100, (renderer.position / renderer.duration) * 100)}%` }}
                  />
                </div>
                <div className="progress-time">
                  <span>{formatDuration(renderer.position)}</span>
                  <span>{formatDuration(renderer.duration)}</span>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="controls">
        {renderer.transportState === 'PLAYING' ? (
          <button className="btn primary" onClick={pause}>⏸ Pause</button>
        ) : (
          <button className="btn primary" onClick={play} disabled={queue.length === 0 && !currentTrack}>
            ▶ Play
          </button>
        )}
        <button className="btn" onClick={stop}>⏹ Stop</button>
        <button className="btn" onClick={next} disabled={queue.length === 0}>⏭ Next</button>
        <label className="volume-control" title={`Volume: ${volumeValue}`}>
          <span className="volume-icon" aria-hidden="true">🔊</span>
          <input
            type="range"
            className="volume-slider"
            min="0"
            max="100"
            value={volumeValue}
            onChange={onVolumeInput}
            disabled={upnpStatus !== 'connected'}
          />
          <span className="volume-value">{volumeValue}</span>
        </label>
        <button className="btn danger" onClick={clearQueue} disabled={queue.length === 0}>🗑 Clear</button>
      </div>

      <div className="radio-row">
        <label className="toggle-label" onClick={() => updateRadio({ enabled: !radioCfg.enabled })}>
          <span>📻 radio</span>
          <span className={`toggle-switch ${radioCfg.enabled ? 'on' : ''}`}>
            <span className="toggle-knob" />
          </span>
        </label>
        {radioCfg.enabled && (
          <>
            <label className="toggle-label" onClick={() => updateRadio({ similarSongs: !radioCfg.similarSongs })}>
              <span>similar songs</span>
              <span className={`toggle-switch ${radioCfg.similarSongs ? 'on' : ''}`}>
                <span className="toggle-knob" />
              </span>
            </label>
            <label className="toggle-label" onClick={() => updateRadio({ similarArtists: !radioCfg.similarArtists })}>
              <span>similar artists</span>
              <span className={`toggle-switch ${radioCfg.similarArtists ? 'on' : ''}`}>
                <span className="toggle-knob" />
              </span>
            </label>
            <label className="toggle-label" onClick={() => updateRadio({ similarGenres: !radioCfg.similarGenres })}>
              <span>similar genres</span>
              <span className={`toggle-switch ${radioCfg.similarGenres ? 'on' : ''}`}>
                <span className="toggle-knob" />
              </span>
            </label>
            <label className="num-label">
              <span>fill at ≤</span>
              <input type="number" min="1" max="50" className="num-input"
                value={radioCfg.queueThreshold}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  updateRadio({ queueThreshold: Number.isFinite(n) ? Math.max(1, Math.min(50, n)) : 5 });
                }} />
            </label>
            <label className="num-label">
              <span>add</span>
              <input type="number" min="1" max="50" className="num-input"
                value={radioCfg.batchSize}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  updateRadio({ batchSize: Number.isFinite(n) ? Math.max(1, Math.min(50, n)) : 10 });
                }} />
              <span>at a time</span>
            </label>
          </>
        )}
      </div>

      <div className="queue-section">
        <h2 className="queue-heading">
          Up Next ({queue.length} {queue.length === 1 ? 'track' : 'tracks'})
        </h2>

        {queue.length === 0 ? (
          <div className="empty-message">Queue is empty. Search and add tracks!</div>
        ) : (
          <div className="queue">
            {queue.map((track, idx) => (
              <div key={`${track.id}-${idx}`} className="queue-item"
                draggable
                onDragStart={() => handleDragStart(idx, track.id)}
                onDragOver={(e) => handleDragOver(e, idx)}
                onDrop={handleDrop}
              >
                <span className="drag-handle">⠿</span>
                <img src={`${API}/cover/${track.coverArt}`} alt="" className="queue-art"
                  loading="lazy" decoding="async"
                  onLoad={(e) => (e.target.style.display = '')}
                  onError={(e) => (e.target.style.display = 'none')} />
                <div className="queue-info">
                  <div className="queue-track-title">{track.title}</div>
                  <div className="queue-artist">{track.artist} — {track.album}</div>
                </div>
                <div className="queue-duration">{formatDuration(track.duration)}</div>
                <button className="remove-btn" onClick={() => removeTrack(idx)} title="Remove">✕</button>
              </div>
            ))}
          </div>
        )}

        {radioFilling && (
          <div className="queue-spinner">
            <span className="spinner" />
            <span>queueing more…</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
