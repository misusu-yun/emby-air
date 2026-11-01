package httpapi

import "net/http"

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePublicInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ServerName": s.cfg.ServerName,
		"Version":    "4.10.2.0",
		"Id":         s.serverID(),
	})
}

// handleSystemInfo 是可选的扩展端点（已鉴权）；返回服务端概要与库统计。
func (s *Server) handleSystemInfo(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"ServerName":        s.cfg.ServerName,
		"Version":           "4.10.2.0",
		"Id":                s.serverID(),
		"OperatingSystem":   "",
		"HasPendingRestart": false,
	}
	if s.db != nil {
		if st, err := s.db.Stats(r.Context()); err == nil {
			resp["ItemCount"] = st.Items
			resp["MovieCount"] = st.Movies
			resp["SeriesCount"] = st.Series
			resp["EpisodeCount"] = st.Episodes
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
