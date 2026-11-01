package httpapi

import (
	"net/http"
	"net/url"
	"strings"

	"emby-air/internal/settings"
)

func settingsResponse(s settings.Settings) map[string]any {
	return map[string]any{
		"TMDBKeySet": s.TMDBAPIKey != "",
		"TMDBLang":   s.TMDBLang,
		"Proxy":      s.Proxy,
		"NoProxy":    s.NoProxy,
		"ScrapeRate": s.ScrapeRate,
		"AutoScrape": s.AutoScrape,
		"NFOWrite":   s.NFOWrite,
	}
}

// handleWebSettings GET/POST /webui/api/settings：管理员修改 TMDB/代理/刮削配置。
func (s *Server) handleWebSettings(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, settingsResponse(settings.Load(r.Context(), s.db)))
	case http.MethodPost:
		var body struct {
			TMDBAPIKey   string  `json:"TMDBAPIKey"`
			ClearTMDBKey bool    `json:"ClearTMDBKey"`
			TMDBLang     string  `json:"TMDBLang"`
			Proxy        *string `json:"Proxy"`
			NoProxy      *string `json:"NoProxy"`
			ScrapeRate   *int    `json:"ScrapeRate"`
			AutoScrape   *bool   `json:"AutoScrape"`
			NFOWrite     *bool   `json:"NFOWrite"`
		}
		if err := decodeJSONBody(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		cur := settings.Load(r.Context(), s.db)
		if body.ClearTMDBKey {
			cur.TMDBAPIKey = ""
		} else if v := strings.TrimSpace(body.TMDBAPIKey); v != "" {
			cur.TMDBAPIKey = v
		}
		if v := strings.TrimSpace(body.TMDBLang); v != "" {
			cur.TMDBLang = v
		}
		if body.Proxy != nil {
			cur.Proxy = strings.TrimSpace(*body.Proxy)
		}
		if cur.Proxy != "" {
			u, err := url.Parse(cur.Proxy)
			if err != nil || u.Scheme == "" || u.Host == "" {
				writeError(w, http.StatusBadRequest, "代理地址格式应为 http(s)://host:port")
				return
			}
		}
		if body.NoProxy != nil {
			cur.NoProxy = strings.TrimSpace(*body.NoProxy)
		}
		if body.ScrapeRate != nil {
			if *body.ScrapeRate < 1 || *body.ScrapeRate > 15 {
				writeError(w, http.StatusBadRequest, "刮削速率必须在 1–15 req/s")
				return
			}
			cur.ScrapeRate = *body.ScrapeRate
		}
		if body.AutoScrape != nil {
			cur.AutoScrape = *body.AutoScrape
		}
		if body.NFOWrite != nil {
			cur.NFOWrite = *body.NFOWrite
		}
		if err := settings.Save(r.Context(), s.db, cur); err != nil {
			writeError(w, http.StatusInternalServerError, "保存设置失败")
			return
		}
		if s.cfg.OnSettingsChanged != nil {
			s.cfg.OnSettingsChanged()
		}
		writeJSON(w, http.StatusOK, settingsResponse(settings.Load(r.Context(), s.db)))
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
