package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

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

type Handler struct {
	mu       sync.RWMutex
	store    store.Store
	wsMu     sync.RWMutex
	wsConns  map[string]*websocket.Conn
	upgrader websocket.Upgrader
}

func NewHandler(st store.Store) *Handler {
	return &Handler{
		store:   st,
		wsConns: make(map[string]*websocket.Conn),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (ts *Handler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := ts.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	peerID := r.URL.Query().Get("peer_id")

	ts.wsMu.Lock()
	ts.wsConns[peerID] = conn
	ts.wsMu.Unlock()
	ts.mu.Lock()
	signals, err := ts.store.PollSignals(context.Background(), peerID)
	ts.mu.Unlock()
	if err == nil {
		for _, sig := range signals {
			notif := map[string]any{
				"method": "incoming_signal",
				"params": map[string]string{
					"from": sig.From,
					"type": sig.Type,
					"data": sig.Data,
				},
			}
			notifData, _ := json.Marshal(notif)
			conn.WriteMessage(websocket.TextMessage, notifData)
		}
	}

	defer func() {
		ts.wsMu.Lock()
		delete(ts.wsConns, peerID)
		ts.wsMu.Unlock()
		conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		ts.handleMessage(peerID, data)
	}
}

func (h *Handler) handleMessage(peerID string, data []byte) {
	var req rpcRequest
	if err := json.Unmarshal(data, &req); err != nil || req.ID == "" || req.Method == "" {
		return
	}

	switch req.Method {
	case rpcMethodSignalRelay:
		h.handleSignalRelay(peerID, req)
	case rpcMethodConnectToPeer:
		h.handleConnectToPeer(peerID, req)
	default:
		h.sendError(peerID, req.ID, fmt.Sprintf("unknown method: %s", req.Method))
	}
}

func (h *Handler) handleSignalRelay(fromPeerID string, req rpcRequest) {
	var params SignalRelayRequest
	if err := json.Unmarshal(req.Params, &params); err != nil {
		h.sendError(fromPeerID, req.ID, "invalid signal_relay params")
		return
	}

	if h.sendNotification(params.TargetID, rpcNotifyIncomingSignal, IncomingSignal{
		From: params.From,
		Type: params.Type,
		Data: params.Data,
	}) {
		h.sendResult(fromPeerID, req.ID)
		return
	}

	sig := model.Signal{From: params.From, Type: params.Type, Data: params.Data}
	if err := h.store.SetSignal(context.Background(), params.TargetID, sig); err != nil {
		h.sendError(fromPeerID, req.ID, fmt.Sprintf("store signal: %v", err))
		return
	}

	h.sendResult(fromPeerID, req.ID)
}

func (h *Handler) handleConnectToPeer(fromPeerID string, req rpcRequest) {
	var params ConnectToPeerRequest
	if err := json.Unmarshal(req.Params, &params); err != nil {
		h.sendError(fromPeerID, req.ID, "invalid connect_to_peer params")
		return
	}

	if h.sendNotification(params.TargetID, rpcNotifyIncomingSignal, IncomingSignal{
		From: fromPeerID,
		Type: "offer",
		Data: params.Offer,
	}) {
		h.sendResult(fromPeerID, req.ID)
		return
	}

	sig := model.Signal{From: fromPeerID, Type: "offer", Data: params.Offer}
	if err := h.store.SetSignal(context.Background(), params.TargetID, sig); err != nil {
		h.sendError(fromPeerID, req.ID, fmt.Sprintf("store signal: %v", err))
		return
	}

	h.sendResult(fromPeerID, req.ID)
}

func (h *Handler) sendNotification(peerID, method string, params any) bool {
	h.wsMu.RLock()
	p, ok := h.wsConns[peerID]
	h.wsMu.RUnlock()

	if !ok || p == nil {
		return false
	}
	raw, _ := json.Marshal(params)
	notif := rpcNotification{Method: method, Params: raw}
	notifData, _ := json.Marshal(notif)

	if err := p.WriteMessage(websocket.TextMessage, notifData); err != nil {
		log.Printf("[webrtc] send notification to %s: %v", peerID, err)
		return false
	}

	return true
}

func (h *Handler) sendResult(peerID, reqID string) {
	h.mu.RLock()
	p, ok := h.wsConns[peerID]
	h.mu.RUnlock()

	if !ok || p == nil {
		return
	}

	resp := rpcResponse{ID: reqID, Result: json.RawMessage("{}")}
	data, _ := json.Marshal(resp)
	p.WriteMessage(websocket.TextMessage, data)
}

func (h *Handler) sendError(peerID, reqID, errMsg string) {
	h.mu.RLock()
	p, ok := h.wsConns[peerID]
	h.mu.RUnlock()

	if !ok || p == nil {
		return
	}

	resp := rpcResponse{ID: reqID, Error: errMsg}
	data, _ := json.Marshal(resp)
	p.WriteMessage(websocket.TextMessage, data)
}

func (h *Handler) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, p := range h.wsConns {
		p.Close()
		delete(h.wsConns, id)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
