package httpapi

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"emby-air/internal/model"
)

// handlePlaybackInfo 对应 GET/POST /emby/Items/{Id}/PlaybackInfo。
// STRM 场景：只给直链，SupportsTranscoding 恒为 false。
func (s *Server) handlePlaybackInfo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	it, err := s.db.GetItem(r.Context(), id)
	if err != nil || it == nil {
		if err != nil && !isNotFound(err) {
			writeError(w, http.StatusInternalServerError, "cannot load item")
			return
		}
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), it.Id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	// 诊断：客户端在 PlaybackInfo 里是否携带了 StartTimeTicks（说明它自己知道续播点）。
	if r.Method == http.MethodPost {
		var body struct {
			StartTimeTicks int64  `json:"StartTimeTicks"`
			MediaSourceId  string `json:"MediaSourceId"`
		}
		if err := decodeJSONBody(r, &body); err == nil {
			s.trace.add(map[string]any{
				"ts": time.Now().UnixMilli(), "path": "/PlaybackInfo",
				"item": it.Name, "start": body.StartTimeTicks, "source": body.MediaSourceId,
			})
		}
	}

	userID := ""
	if u := userFrom(r); u != nil {
		userID = u.Id
	}

	sources := it.MediaSources
	if len(sources) == 0 {
		// 兜底：由条目自身构造一个直链来源。
		if strings.TrimSpace(it.Path) == "" {
			writeError(w, http.StatusNotFound, "no playable source")
			return
		}
		target := s.playbackTarget(r, it)
		if strings.TrimSpace(target) == "" {
			target = it.Path
		}
		src := model.MediaSource{
			Id:           it.Id,
			Name:         it.Name,
			Path:         it.Path,
			LocalPath:    it.Path,
			Container:    directContainer(target),
			RunTimeTicks: it.RunTimeTicks,
		}
		if isRemoteURL(target) {
			src.Protocol, src.IsRemote = "Http", true
		} else {
			src.Protocol, src.IsRemote = "File", false
		}
		sources = []model.MediaSource{src}
	}
	attachExternalSubtitles(sources, id)
	if s.cfg.SubtitleCache != "" {
		tmp := *it
		tmp.MediaSources = sources
		s.attachEmbeddedSubtitles(r.Context(), &tmp)
		sources = tmp.MediaSources
	}
	appendSourceSizes(sources)
	popcorn := s.isPopcornClient(r)
	out := make([]model.MediaSource, len(sources))
	for i, ms := range sources {
		if !popcorn {
			// 一般客户端用解码后的可读 URL；爆米花桌面版对非 ASCII 直链处理不稳，保持原始编码。
			ms.Path = prettyURL(ms.Path)
		}
		ms.SupportsDirectPlay = true
		ms.SupportsDirectStream = true
		ms.SupportsTranscoding = false
		// Container 兜底：DB/构造出的值可能是空的或 "strm"（不是真实容器），
		// 一律重新推导（推不出来就置空），绝不把 "strm" 下发给客户端。
		ms.Container = mediaContainer(ms.Container, ms.Path)
		out[i] = ms
	}
	if popcorn || isVidHub(r) {
		// 爆米花和 VidHub 电视版都需要标准 DirectStreamUrl；
		// Filmly 仍按自己的前缀规则，VidHub 使用 /emby/Videos/...。
		fillDirectStreamURLs(r, out, id)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"MediaSources":        out,
		"PlaySessionId":       playbackSessionID(userID, id),
		"SupportsTranscoding": false,
	})
}

// playbackSessionID 由用户+条目派生稳定会话 ID（同一播放请求复用同一 ID）。
func playbackSessionID(userID, itemID string) string {
	sum := sha1.Sum([]byte("play:" + userID + ":" + itemID))
	return hex.EncodeToString(sum[:12])
}

// directContainer 从直链或文件路径推出容器名：优先 file_name 查询参数（云盘直链常见），
// 其次路径扩展名。只接受白名单扩展名；"strm" 从来不是真实容器，永远不返回。
func directContainer(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	candidates := []string{p}
	if u, err := url.Parse(p); err == nil {
		if fn := u.Query().Get("file_name"); fn != "" {
			candidates = append([]string{fn}, candidates...)
		}
		if u.Path != "" {
			candidates = append(candidates, u.Path)
		}
	}
	for _, c := range candidates {
		// 去掉 query / fragment。
		if i := strings.IndexAny(c, "?#"); i >= 0 {
			c = c[:i]
		}
		if i := strings.LastIndexByte(c, '.'); i >= 0 && i < len(c)-1 {
			ext := strings.ToLower(c[i+1:])
			if len(ext) <= 5 && isAlnum(ext) && videoContainers[ext] {
				return ext
			}
		}
	}
	return ""
}

// videoContainers 是允许作为容器下发的扩展名白名单。
var videoContainers = map[string]bool{
	"mkv": true, "mp4": true, "m4v": true, "ts": true, "m2ts": true, "mts": true,
	"avi": true, "mov": true, "flv": true, "webm": true, "wmv": true, "mpg": true,
	"mpeg": true, "iso": true, "rmvb": true, "rm": true, "vob": true, "ogv": true,
	"3gp": true, "m2v": true,
}

// mediaContainer 返回可下发的容器名：已有有效值时原样保留，
// 为空或 "strm" 时从直链推断；推断不出返回空串（宁可不给，也不给错）。
func mediaContainer(container, path string) string {
	c := strings.ToLower(strings.TrimSpace(container))
	if videoContainers[c] {
		return c
	}
	return directContainer(path)
}

// isRemoteURL 判断播放目标是远端 HTTP(S) 直链还是本地文件。
func isRemoteURL(p string) bool {
	lp := strings.ToLower(strings.TrimSpace(p))
	return strings.HasPrefix(lp, "http://") || strings.HasPrefix(lp, "https://")
}

func isAlnum(s string) bool {
	for i := range len(s) {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// handleVideoStream 对应 GET/HEAD /emby/Videos/{Id}/stream，
// 302 到 STRM 文件内容里的直链。Location 原样输出，不做任何改写，也不用请求 Host。
func (s *Server) handleVideoStream(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	it, err := s.db.GetItem(r.Context(), id)
	if err != nil || it == nil {
		if err != nil && !isNotFound(err) {
			writeError(w, http.StatusInternalServerError, "cannot load item")
			return
		}
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), it.Id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	target := s.playbackTarget(r, it)
	if target == "" {
		writeError(w, http.StatusNotFound, "no stream url")
		return
	}

	// 网易爆米花对 302 的处理不稳定：可配置成由服务端代理直链（流式转发，不缓存整段）。
	// bridge 模式下 127.0.0.1 在客户端不可达，也必须由服务端代理。
	if (s.cfg.PopcornProxyStream && s.isPopcornClient(r)) || isVidHub(r) || s.cfg.UpstreamHost != "" {
		// VidHub 电视版无法访问容器内的 127.0.0.1 直链，改为服务端代理；
		// Range/206 由 proxyStream 透传。
		s.proxyStream(w, r, target)
		return
	}
	w.Header().Set("Location", prettyURL(target))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusFound)
}

// playbackTarget 取直链：优先 MediaSources，其次条目 Path。
func (s *Server) playbackTarget(r *http.Request, it *model.Item) string {
	want := strings.TrimSpace(getCI(r, "MediaSourceId"))
	for _, ms := range it.MediaSources {
		if want != "" && ms.Id != want {
			continue
		}
		if p := strings.TrimSpace(ms.Path); p != "" {
			return p
		}
	}
	// 指定的 MediaSourceId 不存在时回落到第一个来源，避免客户端播放直接失败。
	for _, ms := range it.MediaSources {
		if p := strings.TrimSpace(ms.Path); p != "" {
			return p
		}
	}
	// STRM：条目 Path 指向本地 .strm 文件，直链在文件内容里；必须读出来，
	// 不能把本地文件路径当成播放地址（否则客户端会 302 到本地路径）。
	p := strings.TrimSpace(it.Path)
	if strings.EqualFold(filepath.Ext(p), ".strm") {
		if b, err := os.ReadFile(p); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if v := strings.TrimSpace(line); v != "" && !strings.HasPrefix(v, "#") {
					return v
				}
			}
		}
		return ""
	}
	return p
}

// prettyURL 把 URL 里“纯非 ASCII 字符”的百分号编码还原成 UTF-8，
// 否则 VidHub 等客户端会把文件名显示成 %E5%9B%BD%E8%AF%AD%E9%85%8D%E9%9F%B3 这种乱码。
// 只还原解码后全部为多字节字符的片段，避免误改保留字符（%2F、空格、查询参数）。
func prettyURL(raw string) string {
	re := regexp.MustCompile(`(?:%[0-9A-Fa-f]{2})+`)
	return re.ReplaceAllStringFunc(raw, func(esc string) string {
		dec, err := url.PathUnescape(esc)
		if err != nil || dec == "" {
			return esc
		}
		for _, c := range []byte(dec) {
			if c < 0x80 {
				return esc
			}
		}
		return dec
	})
}

// proxyStream 流式转发上游直链，保留 Range/206 语义；不把内容读进内存。
func (s *Server) proxyStream(w http.ResponseWriter, r *http.Request, target string) {
	target = s.rewriteUpstreamTarget(target)
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, nil)
	if err != nil {
		writeError(w, http.StatusBadGateway, "bad upstream url")
		return
	}
	for _, h := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	if s.streamClient == nil {
		s.streamClient = &http.Client{Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConnsPerHost:   4,
			ResponseHeaderTimeout: 15 * time.Second,
		}}
	}
	res, err := s.streamClient.Do(req)
	if err != nil {
		log.Printf("代理播放失败 %s：%v", target, err)
		writeError(w, http.StatusBadGateway, "upstream error")
		return
	}
	defer res.Body.Close()
	// 部分客户端（爆米花桌面版）会先发 HEAD；上游不支持 HEAD 时用已知大小兜底，
	// 避免客户端在预检阶段就判定“无法播放”。
	if r.Method == http.MethodHead && res.StatusCode >= 400 {
		ct := contentTypeForURL(target)
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		return
	}
	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified"} {
		if v := res.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	// 上游常常不给 Content-Type 或只给 octet-stream，按扩展名补一个更准确的（放在拷贝之后覆盖）。
	if ct := w.Header().Get("Content-Type"); ct == "" || strings.HasPrefix(ct, "application/octet-stream") {
		if inferred := contentTypeForURL(target); inferred != "" {
			w.Header().Set("Content-Type", inferred)
		}
	}
	w.WriteHeader(res.StatusCode)
	if r.Method == http.MethodHead {
		return
	}
	// io.Copy 使用 32KB 缓冲，内存占用固定，不随文件大小增长。
	_, _ = io.Copy(w, res.Body)
}

// contentTypeForURL 根据直链推断媒体类型：先看 file_name 查询参数（云盘直链常见），
// 再看路径扩展名；上游不给 Content-Type 时用它兜底，避免桌面播放器拒播。
func contentTypeForURL(raw string) string {
	candidates := []string{raw}
	if u, err := url.Parse(raw); err == nil {
		if fn := u.Query().Get("file_name"); fn != "" {
			candidates = append([]string{fn}, candidates...)
		}
		candidates = append(candidates, u.Path)
	}
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if i := strings.IndexAny(c, "?#"); i >= 0 {
			c = c[:i]
		}
		switch strings.ToLower(filepath.Ext(c)) {
		case ".mp4", ".m4v":
			return "video/mp4"
		case ".mkv":
			return "video/x-matroska"
		case ".ts", ".m2ts":
			return "video/mp2t"
		case ".avi":
			return "video/x-msvideo"
		case ".mov":
			return "video/quicktime"
		case ".flv":
			return "video/x-flv"
		case ".webm":
			return "video/webm"
		}
	}
	return "application/octet-stream"
}
