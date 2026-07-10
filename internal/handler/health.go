package handler

import (
	"log/slog"
	"net/http"
)

func Healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// StallSweep is called by Cloud Scheduler (POST /stall-sweep) every 5 minutes.
func (h *Handler) StallSweep(w http.ResponseWriter, r *http.Request) {
	if err := h.process.ProcessStallSweep(r.Context()); err != nil {
		h.logger.WithSpanContext(r.Context()).Error(r.Context(),
			"stall_sweep.error", "stall sweep failed",
			slog.String("error", err.Error()),
			slog.Bool("audit", true),
		)
		http.Error(w, "stall sweep failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
