package httpapi

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"emby-air/internal/model"
)

// externalSubtitle 是一个实际存在的外挂字幕文件。
type externalSubtitle struct {
	Path     string
	Codec    string
	Language string
	Title    string
	Ext      string
}

// externalSubtitleCandidates 返回某个媒体源同目录下的外挂字幕。
// 同时支持 <base>.srt 和 <base>.zh.srt / <base>.chs.srt 这两种常见命名。
func externalSubtitleCandidates(ms model.MediaSource) []externalSubtitle {
	p := strings.TrimSpace(ms.LocalPath)
	if p == "" {
		p = strings.TrimSpace(ms.Path)
	}
	if p == "" || strings.Contains(p, "://") {
		return nil
	}
	ext := filepath.Ext(p)
	if ext == "" {
		return nil
	}
	base := strings.TrimSuffix(p, ext)
	langs := []struct {
		suffix string
		lang   string
		title  string
	}{
		{"", "", ""},
		{".zh", "chi", "中文"},
		{".chs", "chi", "简体中文"},
		{".chi", "chi", "中文"},
		{".zh-CN", "chi", "简体中文"},
		{".zh-Hans", "chi", "简体中文"},
		{".en", "eng", "English"},
		{".eng", "eng", "English"},
	}
	exts := []struct {
		ext   string
		codec string
	}{
		{".srt", "subrip"},
		{".ass", "ass"},
		{".vtt", "webvtt"},
	}
	seen := map[string]bool{}
	var out []externalSubtitle
	for _, l := range langs {
		for _, e := range exts {
			path := base + l.suffix + e.ext
			if seen[path] {
				continue
			}
			if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() && fi.Size() > 0 {
				seen[path] = true
				title := l.title
				if title == "" {
					// 没有语言后缀时，按文件名判断；仍无法判断就用“字幕”。
					low := strings.ToLower(filepath.Base(path))
					switch {
					case strings.Contains(low, "中文"), strings.Contains(low, "chs"), strings.Contains(low, "chi"), strings.Contains(low, "zh"):
						title, l.lang = "中文", "chi"
					case strings.Contains(low, "english"), strings.Contains(low, "eng"):
						title, l.lang = "English", "eng"
					default:
						title = "字幕"
					}
				}
				out = append(out, externalSubtitle{
					Path: path, Codec: e.codec, Language: l.lang, Title: title, Ext: e.ext,
				})
			}
		}
	}
	return out
}

// attachExternalSubtitles 给每个媒体源追加同目录外挂字幕流。
// Index 使用 1000 + sourceIndex*100 + n，避免与 Video(0)/Audio(1) 冲突。
func attachExternalSubtitles(sources []model.MediaSource, itemID string) {
	for si := range sources {
		cands := externalSubtitleCandidates(sources[si])
		for n, c := range cands {
			idx := 1000 + si*100 + n
			stream := model.MediaStream{
				Index:          idx,
				Type:           "Subtitle",
				Codec:          c.Codec,
				Language:       c.Language,
				Title:          c.Title,
				DisplayTitle:   c.Title + "（外挂）",
				IsExternal:     true,
				DeliveryMethod: "External",
				DeliveryUrl:    fmt.Sprintf("/emby/Videos/%s/Subtitles/%d/Stream?MediaSourceId=%s", itemID, idx, sources[si].Id),
			}
			sources[si].MediaStreams = append(sources[si].MediaStreams, stream)
		}
	}
}

// handleSubtitle GET/HEAD /emby/Videos/{Id}/Subtitles/{Index}/Stream
// 以及 /Videos/... 无前缀变体。外挂字幕直接读本地文件。
func (s *Server) handleSubtitle(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	id := r.PathValue("Id")
	it, err := s.db.GetItem(r.Context(), id)
	if err != nil || it == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), it.Id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	idx, err := strconv.Atoi(r.PathValue("Index"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid subtitle index")
		return
	}
	wantSource := strings.TrimSpace(getCI(r, "MediaSourceId"))
	for si, ms := range it.MediaSources {
		if wantSource != "" && ms.Id != wantSource {
			continue
		}
		for n, c := range externalSubtitleCandidates(ms) {
			if 1000+si*100+n != idx {
				continue
			}
			s.serveSubtitleFile(w, r, c)
			return
		}
		// 没有外挂字幕时查内嵌字幕探测缓存。
		target := s.subtitleSourceTarget(ms)
		if target == "" || s.cfg.SubtitleCache == "" {
			continue
		}
		cache, _ := s.loadProbeCache(it.Id, ms.Id, target)
		if cache == nil {
			cache = s.probeEmbedded(r.Context(), it.Id, ms.Id, target)
		}
		for n, sub := range cache.Streams {
			if 2000+si*100+n != idx {
				continue
			}
			s.serveEmbeddedSubtitle(w, r, ms, sub)
			return
		}
	}
	writeError(w, http.StatusNotFound, "subtitle not found")
}

func (s *Server) serveSubtitleFile(w http.ResponseWriter, r *http.Request, c externalSubtitle) {
	body, err := os.ReadFile(c.Path)
	if err != nil || len(body) == 0 || len(body) > 16<<20 {
		writeError(w, http.StatusNotFound, "subtitle not found")
		return
	}
	ct := "application/x-subrip; charset=utf-8"
	switch c.Ext {
	case ".ass":
		ct = "text/x-ssa; charset=utf-8"
	case ".vtt":
		ct = "text/vtt; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filepath.Base(c.Path)))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// embeddedSubtitle 是 ffprobe 探测到的内嵌字幕轨。
type embeddedSubtitle struct {
	FfIndex  int    `json:"ff_index"`
	Codec    string `json:"codec"`
	Language string `json:"language"`
	Title    string `json:"title"`
	Default  bool   `json:"default"`
	Forced   bool   `json:"forced"`
}

type embeddedProbeCache struct {
	Target   string             `json:"target"`
	ProbedAt int64              `json:"probed_at"`
	Streams  []embeddedSubtitle `json:"streams"`
	Error    string             `json:"error,omitempty"`
}

const subtitleProbeTTL = 24 * time.Hour

// enrichItemSubtitles 在详情/播放信息中补字幕。
// probe=true 时，在没有外挂字幕的媒体源上懒加载探测内嵌字幕；列表请求传 false，避免扫描大量远端文件。
func (s *Server) enrichItemSubtitles(ctx context.Context, it *model.Item, probe bool) {
	if it == nil {
		return
	}
	attachExternalSubtitles(it.MediaSources, it.Id)
	if !probe {
		return
	}
	s.attachEmbeddedSubtitles(ctx, it)
}

// attachEmbeddedSubtitles 对没有外挂字幕的源执行 ffprobe，并追加字幕流。
func (s *Server) attachEmbeddedSubtitles(ctx context.Context, it *model.Item) {
	if it == nil || s.cfg.SubtitleCache == "" {
		return
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return
	}
	for si := range it.MediaSources {
		ms := &it.MediaSources[si]
		if len(externalSubtitleCandidates(*ms)) > 0 {
			continue // 外挂字幕优先，不重复探测内嵌
		}
		target := s.subtitleSourceTarget(*ms)
		if target == "" {
			continue
		}
		cache, err := s.loadProbeCache(it.Id, ms.Id, target)
		if err != nil || cache == nil {
			cache = s.probeEmbedded(ctx, it.Id, ms.Id, target)
		}
		if cache == nil {
			continue
		}
		for n, sub := range cache.Streams {
			idx := 2000 + si*100 + n
			title := sub.Title
			if title == "" {
				title = subtitleTitle(sub.Language)
			}
			ms.MediaStreams = append(ms.MediaStreams, model.MediaStream{
				Index: idx, Type: "Subtitle", Codec: sub.Codec, Language: sub.Language,
				Title: title, DisplayTitle: title + "（内嵌）", IsDefault: sub.Default, IsForced: sub.Forced,
				// 内嵌字幕交给播放器从原始媒体流中读取；服务端不强制先下载/抽取整部远端视频。
				IsExternal: false, DeliveryMethod: "Embed",
			})
		}
	}
}

func subtitleTitle(lang string) string {
	switch strings.ToLower(lang) {
	case "chi", "zho", "zh", "chs", "cht":
		return "中文"
	case "eng", "en":
		return "English"
	case "jpn", "ja":
		return "日本語"
	case "kor", "ko":
		return "한국어"
	default:
		return "字幕"
	}
}

// subtitleSourceTarget 返回 ffprobe/ffmpeg 可访问的播放目标。
// .strm 先读第一行直链；bridge 模式下把 127.0.0.1 换成 UPSTREAM_HOST。
func (s *Server) subtitleSourceTarget(ms model.MediaSource) string {
	p := strings.TrimSpace(ms.LocalPath)
	if p == "" {
		p = strings.TrimSpace(ms.Path)
	}
	target := p
	if strings.EqualFold(filepath.Ext(p), ".strm") {
		target = readStrmURL(p)
	}
	if target == "" {
		target = strings.TrimSpace(ms.Path)
	}
	return s.rewriteUpstreamTarget(target)
}

func readStrmURL(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v := strings.TrimSpace(line); v != "" && !strings.HasPrefix(v, "#") {
			return v
		}
	}
	return ""
}

// rewriteUpstreamTarget 只在 bridge 场景替换 loopback 主机；host 网络默认不变。
func (s *Server) rewriteUpstreamTarget(raw string) string {
	if raw == "" || s.cfg.UpstreamHost == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return raw
	}
	host := u.Hostname()
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return raw
	}
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	if port != "" {
		u.Host = net.JoinHostPort(s.cfg.UpstreamHost, port)
	} else {
		u.Host = s.cfg.UpstreamHost
	}
	return u.String()
}

func (s *Server) probeCachePath(itemID, sourceID string) string {
	key := sourceID
	if key == "" {
		key = itemID
	}
	key = strings.ReplaceAll(key, "/", "_")
	sum := sha1.Sum([]byte(key))
	return filepath.Join(s.cfg.SubtitleCache, "probe", hex.EncodeToString(sum[:12])+".json")
}

func (s *Server) loadProbeCache(itemID, sourceID, target string) (*embeddedProbeCache, error) {
	p := s.probeCachePath(itemID, sourceID)
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var c embeddedProbeCache
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Target != target || time.Since(time.Unix(c.ProbedAt, 0)) > subtitleProbeTTL {
		return nil, nil
	}
	return &c, nil
}

func (s *Server) probeEmbedded(ctx context.Context, itemID, sourceID, target string) *embeddedProbeCache {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-print_format", "json", "-show_streams", target)
	out, err := cmd.Output()
	c := &embeddedProbeCache{Target: target, ProbedAt: time.Now().Unix()}
	if err != nil {
		c.Error = err.Error()
	} else {
		var data struct {
			Streams []struct {
				Index       int               `json:"index"`
				CodecName   string            `json:"codec_name"`
				CodecType   string            `json:"codec_type"`
				Tags        map[string]string `json:"tags"`
				Disposition map[string]int    `json:"disposition"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(out, &data); err != nil {
			c.Error = err.Error()
		} else {
			for _, st := range data.Streams {
				if st.CodecType != "subtitle" {
					continue
				}
				lang := normalizeSubtitleLang(st.Tags["language"])
				codec := st.CodecName
				if codec == "" {
					codec = "subrip"
				}
				c.Streams = append(c.Streams, embeddedSubtitle{
					FfIndex: st.Index, Codec: codec, Language: lang,
					Title: st.Tags["title"], Default: st.Disposition["default"] == 1,
					Forced: st.Disposition["forced"] == 1,
				})
			}
		}
	}
	_ = os.MkdirAll(filepath.Dir(s.probeCachePath(itemID, sourceID)), 0o755)
	if b, err := json.Marshal(c); err == nil {
		_ = os.WriteFile(s.probeCachePath(itemID, sourceID), b, 0o644)
	}
	return c
}

func normalizeSubtitleLang(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "zh", "zho", "chs", "cht", "zh-cn", "zh-hans", "zh-hant":
		return "chi"
	case "en", "eng":
		return "eng"
	case "ja", "jpn":
		return "jpn"
	case "ko", "kor":
		return "kor"
	}
	return s
}

// serveEmbeddedSubtitle 按需用 ffmpeg 抽取内嵌字幕轨，缓存为 SRT。
func (s *Server) serveEmbeddedSubtitle(w http.ResponseWriter, r *http.Request, ms model.MediaSource, sub embeddedSubtitle) {
	if s.cfg.SubtitleCache == "" {
		writeError(w, http.StatusNotFound, "subtitle not found")
		return
	}
	key := ms.Id
	if key == "" {
		key = "source"
	}
	cacheDir := filepath.Join(s.cfg.SubtitleCache, "extract")
	cachePath := filepath.Join(cacheDir, key+"_"+strconv.Itoa(sub.FfIndex)+".srt")
	if body, err := os.ReadFile(cachePath); err == nil && len(body) > 0 {
		s.serveSubtitleBytes(w, r, body, ".srt")
		return
	}
	target := s.subtitleSourceTarget(ms)
	if target == "" {
		writeError(w, http.StatusNotFound, "subtitle source unavailable")
		return
	}
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		writeError(w, http.StatusServiceUnavailable, "ffmpeg unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
		"-i", target, "-map", "0:"+strconv.Itoa(sub.FfIndex), "-c:s", "srt", "-f", "srt", "pipe:1")
	var buf, stderr bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil || buf.Len() == 0 || buf.Len() > 16<<20 {
		msg := strings.ReplaceAll(stderr.String(), target, "<upstream>")
		if len(msg) > 300 {
			msg = msg[:300]
		}
		log.Printf("内嵌字幕抽取失败 source=%s index=%d: err=%v stderr=%s", ms.Id, sub.FfIndex, err, strings.TrimSpace(msg))
		writeError(w, http.StatusBadGateway, "subtitle extraction failed")
		return
	}
	_ = os.MkdirAll(cacheDir, 0o755)
	tmp := cachePath + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err == nil {
		_ = os.Rename(tmp, cachePath)
	}
	s.serveSubtitleBytes(w, r, buf.Bytes(), ".srt")
}

func (s *Server) serveSubtitleBytes(w http.ResponseWriter, r *http.Request, body []byte, ext string) {
	ct := "application/x-subrip; charset=utf-8"
	if ext == ".ass" {
		ct = "text/x-ssa; charset=utf-8"
	} else if ext == ".vtt" {
		ct = "text/vtt; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// serveSubtitleFile 复用字节响应逻辑，补充下载文件名。
func serveSubtitleFileBytes(w http.ResponseWriter, r *http.Request, body []byte, c externalSubtitle) {
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filepath.Base(c.Path)))
	serveSubtitleBytesCompat(w, r, body, c.Ext)
}

func serveSubtitleBytesCompat(w http.ResponseWriter, r *http.Request, body []byte, ext string) {
	ct := "application/x-subrip; charset=utf-8"
	if ext == ".ass" {
		ct = "text/x-ssa; charset=utf-8"
	} else if ext == ".vtt" {
		ct = "text/vtt; charset=utf-8"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
