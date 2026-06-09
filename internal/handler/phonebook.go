package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

type Phonebook struct {
	Store store.Store
}

func (h *Phonebook) Register(w http.ResponseWriter, r *http.Request) {
	var req model.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	peer, err := h.Store.Upsert(r.Context(), req)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, peer)
}

func (h *Phonebook) Get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "peer id required")
		return
	}

	peer, err := h.Store.Get(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, peer)
}

func (h *Phonebook) GetByKey(w http.ResponseWriter, r *http.Request) {
	login := r.PathValue("login")
	signature := r.URL.Query().Get("signature")
	if login == "" {
		writeError(w, http.StatusBadRequest, "login required")
		return
	}
	if signature == "" {
		writeError(w, http.StatusBadRequest, "signature required")
		return
	}

	peer, err := h.Store.GetByKey(r.Context(), login, signature)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, peer)
}

func (h *Phonebook) Delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "peer id required")
		return
	}

	if err := h.Store.Delete(r.Context(), id); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Phonebook) List(w http.ResponseWriter, r *http.Request) {
	peers, err := h.Store.List(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if peers == nil {
		peers = []model.Peer{}
	}
	writeJSON(w, http.StatusOK, peers)
}

func (h *Phonebook) GetBestPeers(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	n := 10
	if nStr != "" {
		parsed, err := strconv.Atoi(nStr)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid n")
			return
		}
		n = parsed
	}

	peers, err := h.Store.GetBestPeers(r.Context(), n)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if peers == nil {
		peers = []model.Peer{}
	}
	writeJSON(w, http.StatusOK, peers)
}

func (h *Phonebook) Heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "peer id required")
		return
	}

	var body struct {
		TTLSeconds int `json:"ttl_seconds,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	peer, err := h.Store.Heartbeat(r.Context(), id, body.TTLSeconds)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, peer)
}

func (h *Phonebook) RegisterChunk(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("file_id")
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || index < 0 {
		writeError(w, http.StatusBadRequest, "invalid chunk index")
		return
	}

	var req model.RegisterChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if err := h.Store.SetChunkHash(r.Context(), fileID, index, req); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Phonebook) ReportChunk(w http.ResponseWriter, r *http.Request) {
	sourceID := r.PathValue("id")
	if sourceID == "" {
		writeError(w, http.StatusBadRequest, "peer id required")
		return
	}

	var req model.ChunkReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	result, err := h.Store.ReportChunk(r.Context(), sourceID, req)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Phonebook) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrInvalidPeer), errors.Is(err, store.ErrInvalidKey), errors.Is(err, store.ErrKeyTaken), errors.Is(err, store.ErrInvalidChunk), errors.Is(err, store.ErrNoSigningKey):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrInvalidSignature):
		writeError(w, http.StatusUnauthorized, err.Error())
	case errors.Is(err, store.ErrChunkNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
