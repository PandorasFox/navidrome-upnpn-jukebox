package upnp

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hecate/navidrome-jukebox/internal/models"
)

// SSDP multicast address and port
const (
	ssdpAddress = "239.255.255.250:1900"
	ssdpTarget  = "ssdp:all"
)

// M-SEARCH request template
var msearchRequest = []byte(
	"M-SEARCH * HTTP/1.1\r\n" +
		"Host: 239.255.255.250:1900\r\n" +
		"Man: \"ssdp:discover\"\r\n" +
		"MX: 3\r\n" +
		"ST: " + ssdpTarget + "\r\n" +
		"\r\n",
)

// Client handles UPnP AVTransport communication
type Client struct {
	deviceNamePrefix string

	// Discovered state
	deviceURL        string
	avTransportURL   string
	renderControlURL string
	avTransportID    string
	renderControlID  string
}

// DeviceDescription represents a parsed UPnP device description
type DeviceDescription struct {
	XMLName xml.Name `xml:"root"`
	Device  Device   `xml:"device"`
}

type Device struct {
	DeviceType   string    `xml:"deviceType"`
	FriendlyName string    `xml:"friendlyName"`
	Manufacturer string    `xml:"manufacturer"`
	ModelName    string    `xml:"modelName"`
	Services     []Service `xml:"serviceList>service"`
	Devices      []Device  `xml:"deviceList>device"`
}

type Service struct {
	ServiceType string `xml:"serviceType"`
	ServiceID   string `xml:"serviceId"`
	ControlURL  string `xml:"controlURL"`
	EventSubURL string `xml:"eventSubURL,omitempty"`
	SCPDURL     string `xml:"SCPDURL,omitempty"`
}

// NewClient creates a new UPnP client that discovers devices via SSDP
func NewClient(deviceNamePrefix string) *Client {
	return &Client{
		deviceNamePrefix: deviceNamePrefix,
	}
}

// Discover performs SSDP discovery and connects to a matching device
func (c *Client) Discover() error {
	result, dev, err := c.ssdpDiscover()
	if err != nil {
		return err
	}

	// Derive base URL (scheme + host) for resolving relative control URLs
	u, err := url.Parse(result.URL)
	if err != nil {
		return fmt.Errorf("invalid device URL %q: %w", result.URL, err)
	}
	baseURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	c.extractServiceURLs(baseURL, dev)
	c.deviceURL = result.URL

	if c.avTransportURL == "" {
		return fmt.Errorf("device %q matched but no AVTransport service found in description", dev.FriendlyName)
	}

	log.Printf("[upnp] AVTransport URL: %s", c.avTransportURL)
	if c.renderControlURL != "" {
		log.Printf("[upnp] RenderingControl URL: %s", c.renderControlURL)
	}

	return nil
}

// ssdpSearchResult holds parsed SSDP response info
type ssdpSearchResult struct {
	URL  string // device description URL from LOCATION header
	St   string
	Ussn string
}

// ssdpDiscover sends M-SEARCH and listens for responses
func (c *Client) ssdpDiscover() (*ssdpSearchResult, *Device, error) {
	// Send M-SEARCH and listen for responses on the same socket bound to :1900.
	// The receiver responds to the source port of the request (port 1900),
	// so a single listening socket handles both send and receive.
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: 1900})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create UDP listener on :1900: %w", err)
	}
	defer conn.Close()
	log.Printf("[upnp] listening on :1900")

	mcastAddr := &net.UDPAddr{IP: net.ParseIP("239.255.255.250"), Port: 1900}
	_, err = conn.WriteToUDP(msearchRequest, mcastAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send M-SEARCH: %w", err)
	}
	log.Printf("[upnp] sent M-SEARCH to 239.255.255.250:1900")

	conn.SetReadDeadline(time.Now().Add(4 * time.Second))

	var results []ssdpSearchResult

	for {
		buf := make([]byte, 4096)
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			log.Printf("[upnp] read error: %v", err)
			break
		}
		log.Printf("[upnp] received %d bytes from %s", n, addr.String())

		response := string(buf[:n])
		result := parseSSDPResponse(response)
		if result == nil {
			log.Printf("[upnp] response from %s did not parse as valid SSDP", addr.String())
			continue
		}

		results = append(results, *result)
		log.Printf("[upnp] discovered device: location=%s st=%s usn=%s", result.URL, result.St, result.Ussn)
	}

	if len(results) == 0 {
		return nil, nil, fmt.Errorf("no UPnP media renderers found on the network")
	}

	// Filter by name prefix
	for _, r := range results {
		dev, err := fetchDeviceDescription(r.URL)
		if err != nil {
			log.Printf("[upnp] failed to fetch device description from %s: %v", r.URL, err)
			continue
		}

		log.Printf("[upnp] device at %s: friendlyName=%q modelName=%q manufacturer=%q", r.URL, dev.FriendlyName, dev.ModelName, dev.Manufacturer)

		if c.matches(dev) {
			log.Printf("[upnp] matched device: %q", dev.FriendlyName)
			return &r, dev, nil
		}

		log.Printf("[upnp] device %q did not match filter %q", dev.FriendlyName, c.deviceNamePrefix)
	}

	return nil, nil, fmt.Errorf("no UPnP media renderer matching %q found on the network", c.deviceNamePrefix)
}

// parseSSDPResponse parses an SSDP M-SEARCH response
func parseSSDPResponse(raw string) *ssdpSearchResult {
	lines := strings.Split(raw, "\r\n")
	if len(lines) < 1 {
		return nil
	}

	// First line should be the status line
	if !strings.HasPrefix(lines[0], "HTTP/1.1 200 OK") && !strings.HasPrefix(lines[0], "HTTP/1.1 500 Internal Server Error") {
		return nil
	}

	result := &ssdpSearchResult{}

	for i := 1; i < len(lines); i++ {
		line := lines[i]
		if line == "" {
			break
		}

		key, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		switch strings.ToLower(key) {
		case "location":
			result.URL = val
		case "st":
			result.St = val
		case "usn":
			result.Ussn = val
		}
	}

	if result.URL == "" {
		return nil
	}

	return result
}

// matches checks if a device matches the configured name prefix
func (c *Client) matches(dev *Device) bool {
	if c.deviceNamePrefix == "" {
		return true
	}

	prefix := strings.ToLower(c.deviceNamePrefix)
	name := strings.ToLower(dev.FriendlyName)
	model := strings.ToLower(dev.ModelName)

	return strings.Contains(name, prefix) || strings.Contains(model, prefix)
}

// fetchDeviceDescription fetches and parses a device description
func fetchDeviceDescription(url string) (*Device, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var desc DeviceDescription
	if err := xml.Unmarshal(body, &desc); err != nil {
		return nil, err
	}

	return &desc.Device, nil
}

// extractServiceURLs extracts control URLs from device description,
// traversing sub-devices (Yamaha receivers nest services under sub-devices).
func (c *Client) extractServiceURLs(baseURL string, dev *Device) {
	baseURL = strings.TrimSuffix(baseURL, "/")

	for _, svc := range dev.Services {
		switch {
		case strings.Contains(svc.ServiceType, "AVTransport"):
			c.avTransportURL = resolveControlURL(baseURL, svc.ControlURL)
			c.avTransportID = svc.ServiceID
			log.Printf("[upnp] found AVTransport: type=%s control=%s", svc.ServiceType, c.avTransportURL)
		case strings.Contains(svc.ServiceType, "RenderingControl"):
			c.renderControlURL = resolveControlURL(baseURL, svc.ControlURL)
			c.renderControlID = svc.ServiceID
			log.Printf("[upnp] found RenderingControl: type=%s control=%s", svc.ServiceType, c.renderControlURL)
		}
	}

	// Recurse into sub-devices
	for i := range dev.Devices {
		c.extractServiceURLs(baseURL, &dev.Devices[i])
	}
}

// resolveControlURL resolves a possibly-relative control URL against the base.
func resolveControlURL(baseURL, controlURL string) string {
	if strings.HasPrefix(controlURL, "http://") || strings.HasPrefix(controlURL, "https://") {
		return controlURL
	}
	if !strings.HasPrefix(controlURL, "/") {
		controlURL = "/" + controlURL
	}
	return baseURL + controlURL
}

// DIDLItem creates a DIDL-Lite metadata block for a track
func DIDLItem(track models.QueueItem, streamURL string) string {
	protocolInfo := "http-get:*:audio/mpeg:*"

	albumArt := ""
	if track.CoverArt != "" {
		albumArt = fmt.Sprintf("\n    <upnp:albumArtURI>__COVER_ART_%s__</upnp:albumArtURI>", track.CoverArt)
	}

	return fmt.Sprintf(`<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">
  <item id="%s" parentID="0" restricted="1">
    <dc:title>%s</dc:title>
    <dc:creator>%s</dc:creator>
    <upnp:artist>%s</upnp:artist>
    <upnp:album>%s</upnp:album>
    <upnp:class>object.item.audioItem.musicTrack</upnp:class>%s
    <res protocolInfo="%s">%s</res>
  </item>
</DIDL-Lite>`,
		track.ID,
		escapeXML(track.Title),
		escapeXML(track.Artist),
		escapeXML(track.Artist),
		escapeXML(track.Album),
		albumArt,
		protocolInfo,
		escapeXML(streamURL))
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "'", "&#39;")
	return s
}

// TransportAction represents a UPnP AVTransport action
type TransportAction string

const (
	ActionPlay                  TransportAction = "Play"
	ActionPause                 TransportAction = "Pause"
	ActionStop                  TransportAction = "Stop"
	ActionSetAVTransportURI     TransportAction = "SetAVTransportURI"
	ActionSetNextAVTransportURI TransportAction = "SetNextAVTransportURI"
	ActionGetPositionInfo       TransportAction = "GetPositionInfo"
	ActionGetVolume             TransportAction = "GetVolume"
	ActionSetVolume             TransportAction = "SetVolume"
)

// ControlPoint handles AVTransport control
type ControlPoint struct {
	client *Client
}

// NewControlPoint creates a new control point for the client
func (c *Client) NewControlPoint() *ControlPoint {
	return &ControlPoint{client: c}
}

// Play sends the Play command
func (cp *ControlPoint) Play(speed string) error {
	if speed == "" {
		speed = "1"
	}
	return cp.action(ActionPlay, map[string]string{
		"InstanceID": "0",
		"Speed":      speed,
	})
}

// Pause sends the Pause command
func (cp *ControlPoint) Pause() error {
	return cp.action(ActionPause, map[string]string{
		"InstanceID": "0",
	})
}

// Stop sends the Stop command
func (cp *ControlPoint) Stop() error {
	return cp.action(ActionStop, map[string]string{
		"InstanceID": "0",
	})
}

// SetAVTransportURI sets the current track to play
func (cp *ControlPoint) SetAVTransportURI(instanceID string, uri, meta string) error {
	return cp.action(ActionSetAVTransportURI, map[string]string{
		"InstanceID":         instanceID,
		"CurrentURI":         uri,
		"CurrentURIMetaData": meta,
	})
}

// SetNextAVTransportURI queues the next track (for gapless)
func (cp *ControlPoint) SetNextAVTransportURI(instanceID string, uri, meta string) error {
	return cp.action(ActionSetNextAVTransportURI, map[string]string{
		"InstanceID":      instanceID,
		"NextURI":         uri,
		"NextURIMetaData": meta,
	})
}

// GetVolume returns the current master volume on the renderer (0-100).
func (cp *ControlPoint) GetVolume(instanceID string) (int, error) {
	resp, err := cp.actionWithResponse(ActionGetVolume, map[string]string{
		"InstanceID": instanceID,
		"Channel":    "Master",
	})
	if err != nil {
		return 0, err
	}

	open := "<CurrentVolume>"
	close := "</CurrentVolume>"
	if idx := strings.Index(resp, open); idx >= 0 {
		start := idx + len(open)
		end := strings.Index(resp[start:], close)
		if end > 0 {
			var v int
			fmt.Sscanf(resp[start:start+end], "%d", &v)
			return v, nil
		}
	}
	return 0, fmt.Errorf("CurrentVolume not found in GetVolume response")
}

// SetVolume sets the master volume on the renderer (0-100).
func (cp *ControlPoint) SetVolume(instanceID string, volume int) error {
	if volume < 0 {
		volume = 0
	}
	if volume > 100 {
		volume = 100
	}
	return cp.action(ActionSetVolume, map[string]string{
		"InstanceID":    instanceID,
		"Channel":       "Master",
		"DesiredVolume": fmt.Sprintf("%d", volume),
	})
}

// GetPositionInfo retrieves current playback state
func (cp *ControlPoint) GetPositionInfo(instanceID string) (*models.PlaybackState, error) {
	resp, err := cp.actionWithResponse(ActionGetPositionInfo, map[string]string{
		"InstanceID": instanceID,
	})
	if err != nil {
		return nil, err
	}

	return parsePositionInfo(resp)
}

// action sends a UPnP action
func (cp *ControlPoint) action(action TransportAction, args map[string]string) error {
	_, err := cp.actionWithResponse(action, args)
	return err
}

// actionWithResponse sends a UPnP action and returns the response
func (cp *ControlPoint) actionWithResponse(action TransportAction, args map[string]string) (string, error) {
	controlURL := cp.client.avTransportURL
	serviceNS := "urn:schemas-upnp-org:service:AVTransport:1"

	if strings.Contains(string(action), "Volume") || strings.Contains(string(action), "Mute") {
		controlURL = cp.client.renderControlURL
		serviceNS = "urn:schemas-upnp-org:service:RenderingControl:1"
	}

	if controlURL == "" {
		return "", fmt.Errorf("no control URL for action %s", action)
	}

	soapAction := fmt.Sprintf("%s#%s", serviceNS, action)

	var argsXML string
	for k, v := range args {
		argsXML += fmt.Sprintf("<%s>%s</%s>", k, escapeXML(v), k)
	}

	envelope := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:%s xmlns:u="%s">%s</u:%s>
  </s:Body>
</s:Envelope>`, action, serviceNS, argsXML, action)

	req, err := http.NewRequest("POST", controlURL, strings.NewReader(envelope))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", fmt.Sprintf(`"%s"`, soapAction))

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send action %s: %w", action, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("action %s failed with status %d: %s", action, resp.StatusCode, string(body))
	}

	return string(body), nil
}

// parsePositionInfo parses GetPositionInfo response
func parsePositionInfo(body string) (*models.PlaybackState, error) {
	// Response format:
	// <u:GetPositionInfoResponse>
	//   <NextTrackURI>...</NextTrackURI>
	//   <NextTrackMetaData>...</NextTrackMetaData>
	//   <TrackURI>...</TrackURI>
	//   <TrackMetaData>...</TrackMetaData>
	//   <TrackMetadataValid>1</TrackMetadataValid>
	//   <TrackNumber>1</TrackNumber>
	//   <TrackDuration>00:03:45</TrackDuration>
	//   <TrackStartPosition>-1</TrackStartPosition>
	//   <TrackLength>225</TrackLength>
	//   <RelTime>00:01:23</RelTime>
	//   <AbsTime>00:01:23</AbsTime>
	// </u:GetPositionInfoResponse>

	state := &models.PlaybackState{}

	// Extract TransportState separately (it's in a different response)
	// For now, we'll infer it or get it from GetTransportInfo

	// Parse TrackURI
	if idx := strings.Index(body, "<TrackURI>"); idx >= 0 {
		start := idx + len("<TrackURI>")
		end := strings.Index(body[start:], "</TrackURI>")
		if end > 0 {
			state.CurrentURI = body[start : start+end]
		}
	}

	// Parse RelTime (current position)
	if idx := strings.Index(body, "<RelTime>"); idx >= 0 {
		start := idx + len("<RelTime>")
		end := strings.Index(body[start:], "</RelTime>")
		if end > 0 {
			state.Position = parseTime(body[start : start+end])
		}
	}

	// Parse duration: try multiple tag names used by different renderers
	for _, tag := range []string{"TrackDuration", "TrackLength", "MediaDuration"} {
		if state.Duration > 0 {
			break
		}
		open := "<" + tag + ">"
		close := "</" + tag + ">"
		if idx := strings.Index(body, open); idx >= 0 {
			start := idx + len(open)
			end := strings.Index(body[start:], close)
			if end > 0 {
				state.Duration = parseTime(body[start : start+end])
			}
		}
	}

	return state, nil
}

// GetTransportInfo retrieves transport state (PLAYING, PAUSED, STOPPED)
func (cp *ControlPoint) GetTransportInfo(instanceID string) (string, error) {
	resp, err := cp.actionWithResponse(TransportAction("GetTransportInfo"), map[string]string{
		"InstanceID": instanceID,
	})
	if err != nil {
		return "", err
	}

	// Try standard UPnP name first: CurrentTransportState, then fallback to TransportState
	for _, tag := range []string{"CurrentTransportState", "TransportState"} {
		open := "<" + tag + ">"
		close := "</" + tag + ">"
		if idx := strings.Index(resp, open); idx >= 0 {
			start := idx + len(open)
			end := strings.Index(resp[start:], close)
			if end > 0 {
				return resp[start : start+end], nil
			}
		}
	}

	return "", nil
}

// parseTime converts UPnP time format (HH:MM:SS or HH:MM:SS:ms) to seconds
func parseTime(t string) int {
	if t == "" || t == "-1" {
		return 0
	}

	parts := strings.Split(t, ":")
	var total int
	multiplier := 1

	for i := len(parts) - 1; i >= 0; i-- {
		var val int
		fmt.Sscanf(parts[i], "%d", &val)
		total += val * multiplier
		multiplier *= 60
	}

	return total
}
