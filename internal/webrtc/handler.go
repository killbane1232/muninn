package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

const (
	rpcMethodSignalRelay    = "signal_relay"
	rpcMethodConnectToPeer  = "connect_to_peer"
	rpcNotifyIncomingSignal = "incoming_signal"
)

type rpcRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type rpcNotification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type SignalRelayRequest struct {
	TargetID string `json:"target_id"`
	From     string `json:"from"`
	Type     string `json:"type"`
	Data     string `json:"data"`
}

type ConnectToPeerRequest struct {
	TargetID string `json:"target_id"`
	Offer    string `json:"offer"`
}

type IncomingSignal struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type connection struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *connection) write(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	return c.conn.WriteJSON(v)
}

type Handler struct {
	store    store.Store
	wsMu     sync.RWMutex
	wsConns  map[string]*connection
	upgrader websocket.Upgrader
	// RouteSignal is installed before serving; nil selects local delivery.
	RouteSignal func(context.Context, string, model.Signal) error
	PollSignals func(context.Context, string) ([]model.Signal, error)
}

func NewHandler(st store.Store) *Handler {
	return &Handler{store: st, wsConns: make(map[string]*connection), upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}}
}
func (h *Handler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	peerID := r.URL.Query().Get("peer_id")
	if peerID == "" {
		http.Error(w, "peer_id required", http.StatusBadRequest)
		return
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(1 << 20)
	c := &connection{conn: conn}
	h.wsMu.Lock()
	previous := h.wsConns[peerID]
	h.wsConns[peerID] = c
	// Pair queue draining with registration so a concurrent HTTP/mesh signal
	// cannot be queued after this drain and remain stranded until reconnect.
	poll := h.PollSignals
	if poll == nil {
		poll = h.store.PollSignals
	}
	signals, err := poll(r.Context(), peerID)
	if err == nil {
		for _, sig := range signals {
			if err := c.write(notification(sig)); err != nil {
				_ = h.store.SetSignal(r.Context(), peerID, sig)
			}
		}
	}
	h.wsMu.Unlock()
	if previous != nil {
		previous.conn.Close()
	}
	defer func() {
		h.wsMu.Lock()
		if h.wsConns[peerID] == c {
			delete(h.wsConns, peerID)
		}
		h.wsMu.Unlock()
		conn.Close()
	}()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		h.handleMessage(r.Context(), peerID, data)
	}
}
func notification(sig model.Signal) any {
	return struct {
		Method string       `json:"method"`
		Params model.Signal `json:"params"`
	}{rpcNotifyIncomingSignal, sig}
}

// DeliverLocal never forwards to another Muninn. Mesh RPCs use this entry point.
func (h *Handler) DeliverLocal(ctx context.Context, peerID string, sig model.Signal) error {
	if peerID == "" || sig.From == "" || (sig.Type != "offer" && sig.Type != "answer" && sig.Type != "candidate") {
		return store.ErrInvalidChunk
	}
	h.wsMu.RLock()
	defer h.wsMu.RUnlock()
	if c := h.wsConns[peerID]; c != nil {
		if err := c.write(notification(sig)); err == nil {
			return nil
		}
		c.conn.Close()
	}
	return h.store.SetSignal(ctx, peerID, sig)
}
func (h *Handler) handleMessage(ctx context.Context, peerID string, data []byte) {
	var req rpcRequest
	if json.Unmarshal(data, &req) != nil || req.ID == "" || req.Method == "" {
		return
	}
	var id string
	var sig model.Signal
	switch req.Method {
	case rpcMethodSignalRelay:
		var p SignalRelayRequest
		if json.Unmarshal(req.Params, &p) != nil {
			h.respond(peerID, req.ID, "invalid signal_relay params")
			return
		}
		id = p.TargetID
		sig = model.Signal{From: peerID, Type: p.Type, Data: p.Data}
	case rpcMethodConnectToPeer:
		var p ConnectToPeerRequest
		if json.Unmarshal(req.Params, &p) != nil {
			h.respond(peerID, req.ID, "invalid connect_to_peer params")
			return
		}
		id = p.TargetID
		sig = model.Signal{From: peerID, Type: "offer", Data: p.Offer}
	default:
		h.respond(peerID, req.ID, fmt.Sprintf("unknown method: %s", req.Method))
		return
	}
	sender := h.RouteSignal
	if sender == nil {
		sender = h.DeliverLocal
	}
	if err := sender(ctx, id, sig); err != nil {
		h.respond(peerID, req.ID, err.Error())
		return
	}
	h.respond(peerID, req.ID, "")
}
func (h *Handler) respond(peerID, id, message string) {
	h.wsMu.RLock()
	c := h.wsConns[peerID]
	h.wsMu.RUnlock()
	if c == nil {
		return
	}
	response := rpcResponse{ID: id, Error: message}
	if message == "" {
		response.Result = json.RawMessage("{}")
	}
	_ = c.write(response)
}
func (h *Handler) CloseAll() {
	h.wsMu.Lock()
	defer h.wsMu.Unlock()
	for id, c := range h.wsConns {
		c.conn.Close()
		delete(h.wsConns, id)
	}
}

// IsConnected is used by the mesh to find the current signaling home after a
// client changes servers while the previous directory record is still alive.
func (h *Handler) IsConnected(peerID string) bool {
	h.wsMu.RLock()
	defer h.wsMu.RUnlock()
	return h.wsConns[peerID] != nil
}
