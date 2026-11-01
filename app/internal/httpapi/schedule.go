package httpapi

import (
	"net/http"
	"regexp"
	"strings"
)

const scanScheduleKey = "scan_schedule"

// handleScanSchedule 读写定时扫描计划：{"Time":"02:30"} 或 {"Time":""}（关闭）。
func (s *Server) handleScanSchedule(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"Time": s.db.GetSetting(r.Context(), scanScheduleKey)})
	case http.MethodPost:
		var body struct {
			Time string `json:"Time"`
		}
		if err := decodeJSONBody(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		t := strings.TrimSpace(body.Time)
		if t != "" && !regexp.MustCompile(`^([01]\d|2[0-3]):[0-5]\d$`).MatchString(t) {
			writeError(w, http.StatusBadRequest, "时间格式应为 HH:MM")
			return
		}
		if err := s.db.SetSetting(r.Context(), scanScheduleKey, t); err != nil {
			writeError(w, http.StatusInternalServerError, "保存失败")
			return
		}
		s.recordActivity(r, u.Id, u.Name, "scan_schedule", "", "", `{"time":"`+t+`"}`)
		writeJSON(w, http.StatusOK, map[string]any{"Time": t})
	}
}
