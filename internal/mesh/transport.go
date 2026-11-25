package mesh

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

const maxBody = 4 << 20

func newClient(c Config) *http.Client {
	transport := &http.Transport{
		MaxIdleConns: 64, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute,
		ResponseHeaderTimeout: c.transitTimeout(), TLSHandshakeTimeout: c.timeout(),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("no addresses for mesh node")
			}
			for _, ip := range ips {
				if !allowedIP(ip.IP, c.AllowPrivateNetworks) {
					return nil, fmt.Errorf("mesh address is not allowed")
				}
			}
			// Dial the validated IP, not the hostname: DNS changes cannot bypass the check.
			var last error
			for _, ip := range ips {
				conn, err := (&net.Dialer{Timeout: c.timeout()}).DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
				if err == nil {
					return conn, nil
				}
				last = err
			}
			return nil, last
		},
	}
	return &http.Client{Transport: transport, Timeout: c.transitTimeout(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func mac(secret string, fields ...string) string {
	h := hmac.New(sha256.New, []byte(secret))
	for _, field := range fields {
		fmt.Fprintf(h, "%d:", len(field))
		h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (m *Mesh) wireExchange(ctx context.Context, node, path string, in, out any) error {
	slots := m.slots
	if forwarding, _ := ctx.Value(forwardContextKey{}).(bool); forwarding {
		slots = m.relaySlots
	}
	if lookup, _ := ctx.Value(lookupContextKey{}).(bool); lookup {
		slots = m.lookupSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if len(body) > maxBody {
		return fmt.Errorf("mesh request too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, node+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return err
	}
	nonce := hex.EncodeToString(nonceBytes)
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Muninn-Mode", m.cfg.Mode)
	req.Header.Set("X-Muninn-Nonce", nonce)
	req.Header.Set("X-Muninn-Time", stamp)
	if m.cfg.Mode == "closed" {
		req.Header.Set("X-Muninn-MAC", mac(m.cfg.SharedSecret, req.Method, path, stamp, nonce, string(body)))
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(data) > maxBody {
		return fmt.Errorf("mesh response too large")
	}
	if m.cfg.Mode == "closed" && !hmac.Equal([]byte(resp.Header.Get("X-Muninn-MAC")), []byte(mac(m.cfg.SharedSecret, nonce, strconv.Itoa(resp.StatusCode), string(data)))) {
		return fmt.Errorf("invalid mesh response authentication")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mesh HTTP %d", resp.StatusCode)
	}
	return json.Unmarshal(data, out)
}

func (m *Mesh) serve(fn func(context.Context, []byte) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case m.inbound <- struct{}{}:
			defer func() { <-m.inbound }()
		default:
			http.Error(w, "mesh busy", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "invalid mesh body", http.StatusRequestEntityTooLarge)
			return
		}
		nonce, stamp := r.Header.Get("X-Muninn-Nonce"), r.Header.Get("X-Muninn-Time")
		if r.Header.Get("X-Muninn-Mode") != m.cfg.Mode {
			http.Error(w, "mesh mode mismatch", http.StatusForbidden)
			return
		}
		if m.cfg.Mode == "closed" {
			ts, err := strconv.ParseInt(stamp, 10, 64)
			age := time.Since(time.Unix(ts, 0))
			expected := mac(m.cfg.SharedSecret, r.Method, r.URL.Path, stamp, nonce, string(body))
			if err != nil || age < -time.Minute || age > time.Minute || len(nonce) != 32 || !hmac.Equal([]byte(r.Header.Get("X-Muninn-MAC")), []byte(expected)) {
				http.Error(w, "invalid mesh authentication", http.StatusUnauthorized)
				return
			}
			m.mu.Lock()
			now := time.Now()
			for n, expiry := range m.nonces {
				if now.After(expiry) {
					delete(m.nonces, n)
				}
			}
			_, replay := m.nonces[nonce]
			full := len(m.nonces) >= 8192
			if !replay && !full {
				m.nonces[nonce] = now.Add(2 * time.Minute)
			}
			m.mu.Unlock()
			if replay || full {
				http.Error(w, "mesh replay or capacity limit", http.StatusTooManyRequests)
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), m.cfg.transitTimeout())
		defer cancel()
		data, err := json.Marshal(fn(ctx, body))
		if err != nil || len(data) > maxBody {
			http.Error(w, "mesh response too large", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if m.cfg.Mode == "closed" {
			w.Header().Set("X-Muninn-MAC", mac(m.cfg.SharedSecret, nonce, strconv.Itoa(http.StatusOK), string(data)))
		}
		w.Write(data)
	}
}
