package httpapi

import (
	"context"
	"net/http"
	"time"

	"emby-air/internal/model"
)

// handleWebLogin 对应 POST /webui/login。
// 与 Emby 客户端的 AuthenticateByName 分离：Web 管理界面只允许管理员登录，
// 普通用户仍然可以用客户端正常播放，但拿不到 Web 入口。
func (s *Server) handleWebLogin(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	var body struct {
		Name     string `json:"Name"`
		Pw       string `json:"Pw"`
		Password string `json:"Password"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	pw := body.Pw
	if pw == "" {
		pw = body.Password
	}

	u, hash, err := s.db.UserWithHash(r.Context(), body.Name)
	if err != nil || u == nil || !passwordMatches(u, hash, pw) {
		writeError(w, http.StatusUnauthorized, "用户名或密码错误")
		return
	}
	if u.Policy != nil && u.Policy.IsDisabled {
		writeError(w, http.StatusUnauthorized, "账号已被禁用")
		return
	}
	if !u.IsAdmin {
		writeError(w, http.StatusForbidden, "仅管理员可登录 Web 管理界面")
		return
	}

	tok, err := randomToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot create session")
		return
	}
	ci := identifyClient(r)
	writeCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	err = s.db.CreateTokenKind(writeCtx, tok, u.Id, ci.DeviceID, "web", "Emby Web UI", ci.Version, ci.Device)
	cancel()
	if err != nil {
		s.putMemToken(tok, &model.User{Id: u.Id, Name: u.Name, IsAdmin: true})
	}
	_ = s.db.TouchDevice(r.Context(), storeDevice(u.Id, ci))
	s.recordActivityAs(r, u.Id, u.Name, "login", "", "", `{"channel":"web"}`, "Emby Web UI")

	u.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, map[string]any{
		"AccessToken": tok,
		"User":        u,
		"ServerId":    s.serverID(),
	})
}
