package httpapi

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// ClientInfo 是从请求头里识别出的客户端信息。
// 识别是“尽力而为”：Emby 协议字段可伪造，仅用于展示与审计，不用于授权。
type ClientInfo struct {
	Client   string
	Device   string
	DeviceID string
	Version  string
	Kind     string
	IP       string
}

// identifyClient 按 Emby 协议逐级降级识别客户端：
// X-Emby-Authorization → X-Emby-Client/Device-Id/Device-Name → User-Agent → 弱指纹。
func identifyClient(r *http.Request) ClientInfo {
	ai := parseEmbyAuth(r.Header.Get("X-Emby-Authorization"))
	ci := ClientInfo{Client: ai.Client, Device: ai.Device, DeviceID: ai.DeviceID, Version: ai.Version, IP: clientAddr(r)}

	if ci.Client == "" {
		ci.Client = strings.TrimSpace(r.Header.Get("X-Emby-Client"))
	}
	if ci.Device == "" {
		ci.Device = strings.TrimSpace(r.Header.Get("X-Emby-Device-Name"))
	}
	if ci.DeviceID == "" {
		ci.DeviceID = strings.TrimSpace(r.Header.Get("X-Emby-Device-Id"))
	}
	if ci.Client == "" {
		ci.Client = clientFromUA(r.UserAgent())
	}
	if ci.DeviceID == "" {
		// 弱指纹：同一 NAT 下多设备会撞，前缀 weak: 明确标注不可信。
		sum := sha1.Sum([]byte(r.UserAgent() + "|" + ci.IP))
		ci.DeviceID = "weak:" + hex.EncodeToString(sum[:8])
	}
	if ci.Client == "" {
		ci.Client = "Unknown"
	}
	ci.Kind = guessClientKind(ci.Client, r.UserAgent())
	return ci
}

func clientFromUA(ua string) string {
	l := strings.ToLower(ua)
	switch {
	case strings.Contains(l, "vidhub"):
		return "VidHub"
	case strings.Contains(l, "infuse"):
		return "Infuse"
	case strings.Contains(l, "moonfin"):
		return "Moonfin"
	case strings.Contains(l, "filmly"):
		return "Filmly"
	case strings.Contains(l, "fileball"):
		return "Fileball"
	case strings.Contains(l, "senplayer"):
		return "SenPlayer"
	case strings.Contains(l, "emby"):
		return "Emby"
	case strings.Contains(l, "chrome"), strings.Contains(l, "safari"), strings.Contains(l, "firefox"):
		return "Browser"
	}
	// 通用兜底：从 UA 里取第一个「产品名/版本」的产品名（排除系统/浏览器组件）。
	ignore := map[string]bool{
		"mozilla": true, "applewebkit": true, "safari": true, "chrome": true,
		"gecko": true, "dart": true, "cfnetwork": true, "darwin": true,
		"windows": true, "linux": true, "android": true, "iphone": true,
		"ipad": true, "macintosh": true, "okhttp": true, "gzip": true,
	}
	for _, tok := range strings.Fields(strings.ReplaceAll(ua, "(", " ")) {
		tok = strings.TrimRight(tok, ";,)")
		name, ver, ok := strings.Cut(tok, "/")
		if !ok || name == "" || ver == "" {
			continue
		}
		if ignore[strings.ToLower(name)] {
			continue
		}
		if len(name) > 30 || !uaProductOK(name) {
			continue
		}
		return name
	}
	return ""
}

// uaProductOK 判断产品名是否只由字母数字和常见符号组成。
func uaProductOK(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func guessClientKind(client, ua string) string {
	l := strings.ToLower(client + " " + ua)
	switch {
	case strings.Contains(l, "web ui"), strings.Contains(l, "browser"):
		return "web"
	case strings.Contains(l, "tv"), strings.Contains(l, "androidtv"), strings.Contains(l, "appletv"):
		return "tv"
	case strings.Contains(l, "iphone"), strings.Contains(l, "ipad"), strings.Contains(l, "android"),
		strings.Contains(l, "mobile"), strings.Contains(l, "vidhub"):
		return "mobile"
	case strings.Contains(l, "mac"), strings.Contains(l, "windows"), strings.Contains(l, "linux"):
		return "desktop"
	}
	return "unknown"
}

// remoteIP 取真实客户端 IP（兼容反代）。
func remoteIP(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); ip != "" {
		if i := strings.IndexByte(ip, ','); i > 0 {
			ip = ip[:i]
		}
		return strings.TrimSpace(ip)
	}
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// clientSignature 汇总所有可用来识别客户端的头部（小写）。
func clientSignature(r *http.Request) string {
	parts := []string{
		r.Header.Get("X-Emby-Client"),
		r.Header.Get("User-Agent"),
		r.Header.Get("X-Emby-Authorization"),
		r.Header.Get("X-Emby-Device-Name"),
		r.Header.Get("X-Emby-Device-Id"),
	}
	raw := strings.Join(parts, " ")
	// 网易爆米花会把中文 UA 做 URL 编码（%E7%BD%91%E6%98%93...），
	// 直接比对会漏掉；解码后再匹配。
	if dec, err := url.PathUnescape(raw); err == nil {
		raw = raw + " " + dec
	}
	return strings.ToLower(raw)
}

// isMoonfin 判断 Moonfin 客户端（UA 或 X-Emby-Client 里带 moonfin）。
func isMoonfin(r *http.Request) bool {
	return strings.Contains(clientSignature(r), "moonfin")
}

// isFilmly 判断 Filmly 客户端。与 isPopcornClient 一样基于 clientSignature
// （已做 UA 解码），不只匹配 User-Agent：Filmly 也可能只在 X-Emby-Client 里标识自己。
// isVidHub 判断 VidHub。电视版对 MediaSources 的处理与手机/桌面版不同，
// 需要服务端提供可访问的 DirectStreamUrl 和正确的流标题。
func isVidHub(r *http.Request) bool {
	return strings.Contains(clientSignature(r), "vidhub")
}

func isFilmly(r *http.Request) bool {
	return strings.Contains(clientSignature(r), "filmly")
}

// isPopcornClient 判断是不是网易爆米花；未配置特征时永远返回 false。
func (s *Server) isPopcornClient(r *http.Request) bool {
	if len(s.cfg.PopcornMatch) == 0 {
		return false
	}
	sig := clientSignature(r)
	for _, p := range s.cfg.PopcornMatch {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" && strings.Contains(sig, p) {
			return true
		}
	}
	return false
}

// clientProbe 是最近鉴权请求的客户端特征环形记录（有界内存，约 60KB）。
type clientProbe struct {
	mu  sync.Mutex
	buf []map[string]string
}

func (p *clientProbe) add(rec map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buf) >= 300 {
		copy(p.buf, p.buf[1:])
		p.buf = p.buf[:len(p.buf)-1]
	}
	p.buf = append(p.buf, rec)
}

func (p *clientProbe) snapshot() []map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]string, len(p.buf))
	copy(out, p.buf)
	return out
}

// handleClientProbe 返回最近 40 次请求的客户端特征（管理员），用于确认真实客户端标识。
func (s *Server) handleClientProbe(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	writeJSON(w, http.StatusOK, s.probe.snapshot())
}

// decodeUA 把 URL 编码的 UA 还原成可读文本（只用于诊断展示）。
func decodeUA(ua string) string {
	if dec, err := url.PathUnescape(ua); err == nil {
		return dec
	}
	return ua
}

// sniffSeenCap 是客户端签名集合的容量上限；满了整体清空，内存必须有上限。
const sniffSeenCap = 2048

// sniffSeenSet 是有界的“已见过客户端签名”集合（mutex + map）。
// 只用于“每个新 (ua|client) 组合打印一次日志”，不需要精确历史。
type sniffSeenSet struct {
	mu   sync.Mutex
	cap  int
	keys map[string]struct{}
}

func newSniffSeenSet(capacity int) *sniffSeenSet {
	return &sniffSeenSet{cap: capacity, keys: make(map[string]struct{}, capacity)}
}

// add 记录 key 并返回它是否是首次出现；集合达到上限时先清空再记录
// （宁可偶尔重复打一条日志，也不让内存无界增长）。
func (s *sniffSeenSet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; ok {
		return false
	}
	if s.cap > 0 && len(s.keys) >= s.cap {
		s.keys = make(map[string]struct{}, s.cap)
	}
	s.keys[key] = struct{}{}
	return true
}

// len 返回当前集合大小（测试/诊断用）。
func (s *sniffSeenSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// sniffSeen 记录已见过的客户端签名，避免重复刷日志（有界集合）。
var sniffSeen = newSniffSeenSet(sniffSeenCap)

// sniffClient 在鉴权之前记录所有疑似爆米花的请求（含未认证/API Key），
// 用于确认它到底有没有连到本服务、走的是哪个接口。
func (s *Server) sniffClient(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isPopcornClient(r) {
			// 非爆米花：每个新出现的 (UA, X-Emby-Client) 组合打印一次，便于发现真实标识。
			if !strings.HasPrefix(r.URL.Path, "/webui") {
				ua := truncateRunes(decodeUA(r.UserAgent()), 60)
				cl := truncateRunes(r.Header.Get("X-Emby-Client"), 30)
				key := ua + "|" + cl
				if sniffSeen.add(key) {
					log.Printf("新客户端标识：client=%q ua=%q first=%s %s", cl, ua, r.Method, r.URL.Path)
				}
			}
			next.ServeHTTP(w, r)
			return
		}
		rec := &handleRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		ua := truncateRunes(decodeUA(r.UserAgent()), 70)
		client := truncateRunes(r.Header.Get("X-Emby-Client"), 40)
		log.Printf("爆米花: %s %s -> %d client=%q ua=%q auth=%v",
			r.Method, r.URL.Path, rec.status, client, ua, tokenFrom(r) != "")
		s.probe.add(map[string]string{
			"client": client,
			"ua":     ua,
			"device": truncateRunes(r.Header.Get("X-Emby-Device-Name"), 40),
			"path":   truncateRunes(r.Method+" "+r.URL.Path+" -> "+strconv.Itoa(rec.status), 90),
		})
	})
}
