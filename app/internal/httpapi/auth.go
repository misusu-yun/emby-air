package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"emby-air/internal/model"
)

// authInfo 是从 X-Emby-Authorization 里宽松解析出的客户端信息。
type authInfo struct {
	Client   string
	Device   string
	DeviceID string
	Version  string
	Token    string
}

// parseEmbyAuth 解析形如
//
//	MediaBrowser Client="Emby Web", Device="Chrome", DeviceId="abc", Version="4.8"
//
// 的授权头。规则：字段名大小写不敏感；引号可缺；字段可缺；多余空格/逗号容忍；
// 首个 token（通常 MediaBrowser）忽略。
func parseEmbyAuth(h string) authInfo {
	var out authInfo
	if h == "" {
		return out
	}
	i, n := 0, len(h)
	for i < n {
		// 跳过前导空白与分隔符。
		for i < n && (isSpaceByte(h[i]) || h[i] == ',') {
			i++
		}
		if i >= n {
			break
		}
		// 读 key：直到 '=' 或分隔符。
		ks := i
		for i < n && h[i] != '=' && h[i] != ',' && !isSpaceByte(h[i]) {
			i++
		}
		key := strings.ToLower(strings.TrimSpace(h[ks:i]))
		// 跳过 key 与 '=' 之间的空白。
		for i < n && isSpaceByte(h[i]) {
			i++
		}
		if i >= n || h[i] != '=' {
			// 没有 '=' 的裸 token（如 MediaBrowser）：丢掉，继续扫描后续字段。
			for i < n && (isSpaceByte(h[i]) || h[i] == ',') {
				i++
			}
			continue
		}
		i++ // 跨过 '='
		for i < n && isSpaceByte(h[i]) {
			i++
		}
		var val string
		if i < n && h[i] == '"' {
			i++
			var b strings.Builder
			for i < n && h[i] != '"' {
				if h[i] == '\\' && i+1 < n {
					b.WriteByte(h[i+1])
					i += 2
					continue
				}
				b.WriteByte(h[i])
				i++
			}
			val = strings.TrimSpace(b.String())
			if i < n {
				i++ // 收尾引号
			}
			// 丢弃引号后到下一个逗号前的残留。
			for i < n && h[i] != ',' {
				i++
			}
		} else {
			vs := i
			for i < n && h[i] != ',' {
				i++
			}
			val = strings.TrimSpace(h[vs:i])
		}
		switch key {
		case "client":
			out.Client = val
		case "device":
			out.Device = val
		case "deviceid":
			out.DeviceID = val
		case "version":
			out.Version = val
		case "token":
			out.Token = val
		}
	}
	return out
}

func isSpaceByte(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }

type loginRequest struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
	Pw       string `json:"Pw"` // 部分客户端（含 Emby 旧版）用 Pw 传密码
}

// password 返回客户端实际提交的密码（兼容 Password / Pw 两种字段）。
func (l loginRequest) password() string {
	if l.Password != "" {
		return l.Password
	}
	return l.Pw
}

// handleAuthenticateByName 对应 POST /emby/Users/AuthenticateByName。
// 成功返回 {User, AccessToken, ServerId, SessionInfo}。
func (s *Server) handleAuthenticateByName(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	// body 可能为空或不是合法 JSON；按空凭据处理，由后续校验拒绝。
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req)
	}
	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		writeError(w, http.StatusUnauthorized, "username is required")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	u, hash, err := s.db.UserByName(r.Context(), req.Username)
	if err != nil || u == nil {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	if !passwordMatches(u, hash, req.password()) {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	if u.Policy != nil && u.Policy.IsDisabled {
		writeError(w, http.StatusUnauthorized, "user is disabled")
		return
	}

	ci := identifyClient(r)
	ai := authInfo{Client: ci.Client, Device: ci.Device, DeviceID: ci.DeviceID, Version: ci.Version}

	token, err := randomToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot create session")
		return
	}
	// 令牌落库最多等 2 秒：扫描期间写锁可能被长时间占用，不能因此拖垮登录。
	writeCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	err = s.db.CreateTokenKind(writeCtx, token, u.Id, ci.DeviceID, "client", ci.Client, ci.Version, ci.Device)
	cancel()
	if err != nil {
		s.putMemToken(token, u)
	}
	// 设备与登录活动都是旁路数据，失败不影响登录。
	_ = s.db.TouchDevice(r.Context(), storeDevice(u.Id, ci))
	s.recordActivity(r, u.Id, u.Name, "login", "", "", `{"channel":"client"}`)

	u.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, map[string]any{
		"User":        u,
		"AccessToken": token,
		"ServerId":    s.serverID(),
		"SessionInfo": map[string]any{
			"Id":                    tokenSessionID(token),
			"UserId":                u.Id,
			"UserName":              u.Name,
			"DeviceId":              ai.DeviceID,
			"DeviceName":            ai.Device,
			"Client":                ai.Client,
			"ApplicationVersion":    ai.Version,
			"PlayableMediaTypes":    []string{"Video"},
			"SupportedCommands":     []string{},
			"SupportsRemoteControl": false,
		},
	})
}

// passwordMatches 目前 store 层只暴露密码哈希字符串；空哈希视为无密码账户。
// 具体校验算法在 store 落地后由这里对接（保持单点）。
func passwordMatches(u *model.User, hash, password string) bool {
	if hash == "" {
		return password == "" || !u.HasPassword
	}
	if !u.HasPassword {
		return true
	}
	return verifyPasswordHash(hash, password)
}

// issueToken 为指定用户签发一个持久化令牌（测试与内部管理使用）。
func (s *Server) issueToken(u *model.User) (string, error) {
	tok, err := randomToken()
	if err != nil {
		return "", err
	}
	if err := s.db.CreateToken(context.Background(), tok, u.Id, ""); err != nil {
		return "", err
	}
	return tok, nil
}
