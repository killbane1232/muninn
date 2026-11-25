package mesh

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config is deliberately separate from the client-facing HTTP configuration.
type Config struct {
	Enabled               bool     `json:"enabled"`
	PublicURL             string   `json:"public_url"`
	Mode                  string   `json:"mode"`
	SharedSecret          string   `json:"shared_secret,omitempty"`
	Seeds                 []string `json:"seeds"`
	StateFile             string   `json:"state_file,omitempty"`
	AllowPrivateNetworks  bool     `json:"allow_private_networks"`
	SyncIntervalSeconds   int      `json:"sync_interval_seconds,omitempty"`
	RequestTimeoutSeconds int      `json:"request_timeout_seconds,omitempty"`
	MaxNodes              int      `json:"max_nodes,omitempty"`
	MaxRelayHops          int      `json:"max_relay_hops,omitempty"`
	TransitTimeoutSeconds int      `json:"transit_timeout_seconds,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("mesh config: trailing JSON")
	}
	if cfg.StateFile != "" && !filepath.IsAbs(cfg.StateFile) {
		cfg.StateFile = filepath.Join(filepath.Dir(path), cfg.StateFile)
	}
	return cfg.normalized()
}

func (c Config) normalized() (Config, error) {
	if !c.Enabled {
		return c, nil
	}
	if c.Mode == "" {
		c.Mode = "open"
	}
	if c.Mode != "open" && c.Mode != "closed" {
		return c, fmt.Errorf("mesh mode must be open or closed")
	}
	if c.Mode == "closed" && len(c.SharedSecret) < 32 {
		return c, fmt.Errorf("closed mesh requires a shared_secret of at least 32 bytes")
	}
	if c.Mode == "open" && c.SharedSecret != "" {
		return c, fmt.Errorf("shared_secret requires closed mode")
	}
	var err error
	c.PublicURL, err = normalizeURL(c.PublicURL, c.AllowPrivateNetworks)
	if err != nil {
		return c, fmt.Errorf("mesh public_url: %w", err)
	}
	if c.SyncIntervalSeconds == 0 {
		c.SyncIntervalSeconds = 30
	}
	if c.RequestTimeoutSeconds == 0 {
		c.RequestTimeoutSeconds = 3
	}
	if c.MaxNodes == 0 {
		c.MaxNodes = 64
	}
	if c.SyncIntervalSeconds < 1 || c.RequestTimeoutSeconds < 1 || c.RequestTimeoutSeconds > 5 || c.MaxNodes < 1 || c.MaxNodes > 256 {
		return c, fmt.Errorf("invalid mesh limits (sync >=1s, timeout 1..5s, max_nodes 1..256)")
	}
	if c.MaxRelayHops == 0 {
		c.MaxRelayHops = 3
	}
	if c.TransitTimeoutSeconds == 0 {
		c.TransitTimeoutSeconds = 8
	}
	if c.MaxRelayHops < 1 || c.MaxRelayHops > maxRelayHops || c.TransitTimeoutSeconds < c.RequestTimeoutSeconds || c.TransitTimeoutSeconds > 8 {
		return c, fmt.Errorf("invalid transit limits (relay hops 1..8, transit timeout between request timeout and 8s)")
	}
	if len(c.Seeds) > c.MaxNodes {
		return c, fmt.Errorf("too many mesh seeds")
	}
	c.Seeds = append([]string(nil), c.Seeds...)
	for i, seed := range c.Seeds {
		c.Seeds[i], err = normalizeURL(seed, c.AllowPrivateNetworks)
		if err != nil {
			return c, fmt.Errorf("mesh seed: %w", err)
		}
	}
	return c, nil
}

func (c Config) transitTimeout() time.Duration {
	return time.Duration(c.TransitTimeoutSeconds) * time.Second
}

func (c Config) timeout() time.Duration { return time.Duration(c.RequestTimeoutSeconds) * time.Second }

func normalizeURL(raw string, allowPrivate bool) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid server URL")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return "", fmt.Errorf("server URL must be an http(s) origin without credentials, path, query or fragment")
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if strings.Contains(host, "%") {
		return "", fmt.Errorf("scoped IP addresses are not allowed")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !allowedIP(ip, allowPrivate) {
			return "", fmt.Errorf("server address is not allowed")
		}
	} else if !allowPrivate && (host == "localhost" || strings.HasSuffix(host, ".localhost") || !strings.Contains(host, ".")) {
		return "", fmt.Errorf("private hostname is not allowed")
	}
	port := u.Port()
	if port == "80" && u.Scheme == "http" || port == "443" && u.Scheme == "https" {
		port = ""
	}
	if port != "" {
		if _, err := net.LookupPort("tcp", port); err != nil {
			return "", fmt.Errorf("invalid server port")
		}
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	u.Path = ""
	return u.String(), nil
}

func allowedIP(ip net.IP, allowPrivate bool) bool {
	// Link-local (including cloud metadata), unspecified and multicast are never targets.
	if ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() {
		return allowPrivate
	}
	if !ip.IsGlobalUnicast() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "64:ff9b::/96", "2002::/16"} {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return false
		}
	}
	return true
}
