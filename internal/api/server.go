// Package api exposes StreamHive over HTTP: create topics, produce, consume,
// commit offsets, and a stats feed for the dashboard.
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/portfolio/streamhive/internal/broker"
)

// Server adapts the broker to HTTP.
type Server struct{ b *broker.Broker }

// New builds an API server.
func New(b *broker.Broker) *Server { return &Server{b: b} }

// Routes returns the mux including static files.
func (s *Server) Routes(staticDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/topics", s.handleTopics)
	mux.HandleFunc("/api/produce", s.handleProduce)
	mux.HandleFunc("/api/consume", s.handleConsume)
	mux.HandleFunc("/api/commit", s.handleCommit)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.Handle("/", http.FileServer(http.Dir(staticDir)))
	return mux
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.b.CreateTopic(req.Name, req.Partitions); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "created"})
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	part, off, err := s.b.Produce(req.Topic, req.Key, []byte(req.Value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"partition": part, "offset": off})
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	group := r.URL.Query().Get("group")
	topic := r.URL.Query().Get("topic")
	partition, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	max := 10
	if m := r.URL.Query().Get("max"); m != "" {
		if v, err := strconv.Atoi(m); err == nil {
			max = v
		}
	}
	msgs, err := s.b.Consume(group, topic, partition, max)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Auto-commit the batch on read for the dashboard's convenience.
	if len(msgs) > 0 {
		s.b.Commit(group, topic, partition, msgs[len(msgs)-1].Offset)
	}
	writeJSON(w, http.StatusOK, msgs)
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Group     string `json:"group"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.b.Commit(req.Group, req.Topic, req.Partition, req.Offset)
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.b.Stats())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
