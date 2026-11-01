package httpapi

import (
	"bytes"
	"context"
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
	"emby-air/internal/store"
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
	FfIndex          int     `json:"index"`
	Type             string  `json:"type"`
	Codec            string  `json:"codec"`
	Language         string  `json:"language"`
	Title            string  `json:"title"`
	Default          bool    `json:"default"`
	Forced           bool    `json:"forced"`
	Width            int     `json:"width,omitempty"`
	Height           int     `json:"height,omitempty"`
	Channels         int     `json:"channels,omitempty"`
	BitDepth         int     `json:"bit_depth,omitempty"`
	PixelFormat      string  `json:"pixel_format,omitempty"`
	ColorTransfer    string  `json:"color_transfer,omitempty"`
	ColorPrimaries   string  `json:"color_primaries,omitempty"`
	ColorSpace       string  `json:"color_space,omitempty"`
	AverageFrameRate float64 `json:"average_frame_rate,omitempty"`
}

type embeddedProbeCache struct {
	Target        string             `json:"target"`
	Size          int64              `json:"size"`
	DurationTicks int64              `json:"duration_ticks,omitempty"`
	ETag          string             `json:"etag,omitempty"`
	LastModified  string             `json:"last_modified,omitempty"`
	ProbedAt      int64              `json:"probed_at"`
	CheckedAt     int64              `json:"checked_at,omitempty"`
	Streams       []embeddedSubtitle `json:"streams"`
	Error         string             `json:"error,omitempty"`
}

const (
	subtitleProbeTTL = 24 * time.Hour
	probeRecheckTTL  = 5 * time.Minute
)

// enrichItemSubtitles 在详情/播放信息中补外挂字幕，并复用数据库中的探测结果。
// probe=true 时才允许探测远端媒体；列表请求传 false，避免打开媒体库就批量探测。
func (s *Server) enrichItemSubtitles(ctx context.Context, it *model.Item, probe bool) {
	if it == nil {
		return
	}
	attachExternalSubtitles(it.MediaSources, it.Id)
	if !probe {
		return
	}
	s.attachEmbeddedSubtitles(ctx, it)
	appendSourceSizes(it.MediaSources)
}

// appendSourceSizes 把字节大小拼到版本显示名，同时同步 Video 流标题。
// 只修改响应对象，不修改数据库里的来源名；列表接口保持轻量，不拼大小。
func appendSourceSizes(sources []model.MediaSource) {
	for i := range sources {
		ms := &sources[i]
		if ms.Size <= 0 {
			continue
		}
		label := humanSize(ms.Size)
		if label == "" || strings.Contains(ms.Name, " · "+label) {
			continue
		}
		old := ms.Name
		if old == "" {
			ms.Name = label
		} else {
			ms.Name = old + " · " + label
		}
		for j := range ms.MediaStreams {
			st := &ms.MediaStreams[j]
			if st.Type == "Video" && (st.Title == old || st.Title == "") {
				st.Title, st.DisplayTitle = ms.Name, ms.Name
			}
		}
	}
}

func humanSize(n int64) string {
	const gb = 1024 * 1024 * 1024
	const mb = 1024 * 1024
	if n >= gb {
		return trimSize(fmt.Sprintf("%.2f GB", float64(n)/gb))
	}
	if n >= mb {
		return trimSize(fmt.Sprintf("%.1f MB", float64(n)/mb))
	}
	return fmt.Sprintf("%d KB", (n+1023)/1024)
}

func trimSize(s string) string {
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

// attachEmbeddedSubtitles 优先使用 media_probes 缓存；缓存缺失或 URL 变化时才 ffprobe。
// 外挂字幕存在时仍可使用已缓存的大小/音轨，但不再探测内嵌字幕（外挂优先）。
func (s *Server) attachEmbeddedSubtitles(ctx context.Context, it *model.Item) {
	if it == nil || s.cfg.SubtitleCache == "" {
		return
	}
	for si := range it.MediaSources {
		ms := &it.MediaSources[si]
		target := s.subtitleSourceTarget(*ms)
		if target == "" {
			continue
		}
		hasExternal := len(externalSubtitleCandidates(*ms)) > 0
		cache, _ := s.loadProbeCache(it.Id, ms.Id, target)
		if cache == nil && !hasExternal {
			cache = s.probeEmbedded(ctx, it.Id, ms.Id, target)
		} else if cache == nil {
			// 有外挂字幕时不必 ffprobe，但仍要取上游大小；只做 HEAD，不下载视频。
			size, etag, lastModified, _ := s.headTarget(ctx, target)
			cache = &embeddedProbeCache{Target: target, Size: size, ETag: etag, LastModified: lastModified,
				ProbedAt: time.Now().Unix(), CheckedAt: time.Now().Unix()}
			if s.db != nil {
				_ = s.db.SaveMediaProbe(context.Background(), probeFromCache(it.Id, ms.Id, cache))
			}
		} else if !hasExternal && !hasAVTracks(cache) {
			// 旧缓存可能只有大小，没有音视频轨道；补一次完整探测。
			// 已记录失败且未过 TTL 时不重复拖慢详情页。
			if cache.Error == "" || time.Since(time.Unix(cache.ProbedAt, 0)) > subtitleProbeTTL {
				cache = s.probeEmbedded(ctx, it.Id, ms.Id, target)
			}
		}
		if cache == nil {
			continue
		}
		s.applyProbeCache(ms, cache, hasExternal, si)
		s.scheduleProbeCheck(it.Id, ms.Id, target, cache)
	}
}

func hasAVTracks(cache *embeddedProbeCache) bool {
	if cache == nil {
		return false
	}
	for _, tr := range cache.Streams {
		if tr.Type == "video" || tr.Type == "audio" {
			return true
		}
	}
	return false
}

// applyProbeCache 把探测到的实际视频/音频轨和大小应用到响应。
// 保留外挂字幕；内嵌字幕只在外挂字幕不存在时追加。
func (s *Server) applyProbeCache(ms *model.MediaSource, cache *embeddedProbeCache, hasExternal bool, sourceIndex int) {
	if ms == nil || cache == nil {
		return
	}
	if cache.Size > 0 {
		ms.Size = cache.Size
	}
	if ms.RunTimeTicks == 0 && cache.DurationTicks > 0 {
		ms.RunTimeTicks = cache.DurationTicks
	}
	if !hasAVTracks(cache) {
		return // 只有大小/ETag 的缓存不覆盖合成轨道
	}
	// 只替换合成的 Video/Audio，保留已经追加的外挂字幕。
	kept := make([]model.MediaStream, 0, len(ms.MediaStreams)+len(cache.Streams))
	for _, st := range ms.MediaStreams {
		if st.Type != "Video" && st.Type != "Audio" {
			kept = append(kept, st)
		}
	}
	tracks := make([]model.MediaStream, 0, len(cache.Streams))
	subN := 0
	for _, tr := range cache.Streams {
		switch strings.ToLower(tr.Type) {
		case "video":
			st := model.MediaStream{
				Index: tr.FfIndex, Type: "Video", Codec: tr.Codec, Width: tr.Width, Height: tr.Height,
				BitDepth: tr.BitDepth, PixelFormat: tr.PixelFormat, ColorTransfer: tr.ColorTransfer,
				ColorPrimaries: tr.ColorPrimaries, ColorSpace: tr.ColorSpace,
				AverageFrameRate: float32(tr.AverageFrameRate), IsDefault: tr.Default,
			}
			if ms.Name != "" {
				st.Title, st.DisplayTitle = ms.Name, ms.Name
			} else {
				st.Title, st.DisplayTitle = tr.Title, tr.Title
			}
			if st.VideoRangeType == "" {
				st.VideoRangeType = videoRangeType(tr.ColorTransfer, tr.ColorPrimaries)
				if st.VideoRangeType != "SDR" {
					st.VideoRange = "HDR"
				}
			}
			tracks = append(tracks, st)
		case "audio":
			st := model.MediaStream{
				Index: tr.FfIndex, Type: "Audio", Codec: tr.Codec,
				Language: normalizeSubtitleLang(tr.Language), Title: tr.Title,
				Channels: tr.Channels, IsDefault: tr.Default,
			}
			if st.Title == "" {
				st.Title = subtitleTitle(st.Language)
			}
			st.DisplayTitle = st.Title
			tracks = append(tracks, st)
		case "subtitle":
			if hasExternal {
				continue
			}
			title := tr.Title
			if title == "" {
				title = subtitleTitle(normalizeSubtitleLang(tr.Language))
			}
			st := model.MediaStream{
				Index: 2000 + sourceIndex*100 + subN, Type: "Subtitle", Codec: tr.Codec,
				Language: normalizeSubtitleLang(tr.Language), Title: title,
				DisplayTitle: title + "（内嵌）", IsDefault: tr.Default, IsForced: tr.Forced,
				// 内嵌轨交给播放器从原始媒体流读取，服务端不强制先抽取整部远端视频。
				IsExternal: false, DeliveryMethod: "Embed",
			}
			tracks = append(tracks, st)
			subN++
		}
	}
	if len(tracks) > 0 {
		ms.MediaStreams = append(kept, tracks...)
	}
	if idx := firstAudioIndex(ms.MediaStreams); idx != nil {
		ms.DefaultAudioStreamIndex = idx
	}
}

func firstAudioIndex(streams []model.MediaStream) *int {
	for i := range streams {
		if streams[i].Type == "Audio" && streams[i].IsDefault {
			idx := streams[i].Index
			return &idx
		}
	}
	for i := range streams {
		if streams[i].Type == "Audio" {
			idx := streams[i].Index
			return &idx
		}
	}
	return nil
}

// videoRangeType 根据 ffprobe 色彩字段推断 HDR 类型，避免把 HDR10 当成 SDR。
func videoRangeType(transfer, primaries string) string {
	t := strings.ToLower(transfer)
	p := strings.ToLower(primaries)
	if strings.Contains(t, "smpte2084") || strings.Contains(t, "pq") {
		return "HDR10"
	}
	if strings.Contains(t, "arib-std-b67") || strings.Contains(t, "hlg") {
		return "HLG"
	}
	if strings.Contains(t, "bt2020") || strings.Contains(p, "bt2020") {
		return "HDR10"
	}
	return "SDR"
}

// scheduleProbeCheck 在缓存过期后后台校验 ETag/Last-Modified/大小；不阻塞当前详情页。
func (s *Server) scheduleProbeCheck(itemID, sourceID, target string, cache *embeddedProbeCache) {
	if cache == nil || itemID == "" || sourceID == "" || s.db == nil {
		return
	}
	if cache.CheckedAt > 0 && time.Since(time.Unix(cache.CheckedAt, 0)) < probeRecheckTTL {
		return
	}
	key := itemID + "\x00" + sourceID
	if _, loaded := s.probeChecks.LoadOrStore(key, true); loaded {
		return
	}
	s.startBackground(func() {
		defer s.probeChecks.Delete(key)
		size, etag, lastModified, ok := s.headTarget(s.lifeCtx, target)
		if !ok {
			_ = s.db.MarkMediaProbeChecked(s.lifeCtx, itemID, sourceID, time.Now().Unix())
			return
		}
		unchanged := (cache.Size == 0 || size == 0 || cache.Size == size) &&
			(cache.ETag == "" || etag == "" || cache.ETag == etag) &&
			(cache.LastModified == "" || lastModified == "" || cache.LastModified == lastModified)
		if unchanged {
			if size > 0 && cache.Size == 0 {
				cache.Size = size
				cache.CheckedAt = time.Now().Unix()
				_ = s.db.SaveMediaProbe(s.lifeCtx, probeFromCache(itemID, sourceID, cache))
			} else {
				_ = s.db.MarkMediaProbeChecked(s.lifeCtx, itemID, sourceID, time.Now().Unix())
			}
			return
		}
		_ = s.probeEmbedded(s.lifeCtx, itemID, sourceID, target)
	})
}

func probeFromCache(itemID, sourceID string, c *embeddedProbeCache) store.MediaProbe {
	tracks, _ := json.Marshal(c.Streams)
	return store.MediaProbe{
		ItemID: itemID, SourceID: sourceID, Target: c.Target, Size: c.Size,
		ETag: c.ETag, LastModified: c.LastModified, Tracks: string(tracks), Error: c.Error,
		ProbedAt: c.ProbedAt, CheckedAt: c.CheckedAt,
	}
}

// headTarget 只读取响应头，不下载视频；用于校验上游是否变化或补齐大小。
func (s *Server) headTarget(ctx context.Context, target string) (size int64, etag, lastModified string, ok bool) {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(hctx, http.MethodHead, target, nil)
		if err != nil {
			return 0, "", "", false
		}
		res, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
		if err != nil {
			return 0, "", "", false
		}
		defer res.Body.Close()
		size := res.ContentLength
		if size < 0 {
			size = 0
		}
		return size, strings.TrimSpace(res.Header.Get("ETag")), strings.TrimSpace(res.Header.Get("Last-Modified")), true
	}
	if fi, err := os.Stat(target); err == nil {
		return fi.Size(), "", "", true
	}
	return 0, "", "", false
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

func (s *Server) loadProbeCache(itemID, sourceID, target string) (*embeddedProbeCache, error) {
	if s.db == nil {
		return nil, nil
	}
	p, err := s.db.GetMediaProbe(context.Background(), itemID, sourceID)
	if err != nil || p == nil || p.Target != target {
		return nil, err
	}
	var streams []embeddedSubtitle
	if err := json.Unmarshal([]byte(p.Tracks), &streams); err != nil {
		return nil, err
	}
	return &embeddedProbeCache{
		Target: p.Target, Size: p.Size, ETag: p.ETag, LastModified: p.LastModified,
		ProbedAt: p.ProbedAt, CheckedAt: p.CheckedAt, Streams: streams, Error: p.Error,
	}, nil
}

func (s *Server) probeEmbedded(ctx context.Context, itemID, sourceID, target string) *embeddedProbeCache {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	c := &embeddedProbeCache{Target: target, ProbedAt: time.Now().Unix(), CheckedAt: time.Now().Unix()}

	// HEAD 提供实际文件大小与 ETag/Last-Modified；不下载视频，超时也不会拖住详情页。
	size, etag, lastModified, _ := s.headTarget(ctx, target)
	c.Size, c.ETag, c.LastModified = size, etag, lastModified

	cmd := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", target)
	out, err := cmd.Output()
	if err != nil {
		c.Error = err.Error()
	} else if err := parseProbeOutput(out, c); err != nil {
		c.Error = err.Error()
	}
	if c.Size == 0 {
		// 某些直链 HEAD 不返回 Content-Length，但 ffprobe 的 format.size 通常有值。
		// parseProbeOutput 已写入 c.Size；这里保持 0，由客户端按未知大小处理。
	}
	if s.db != nil {
		_ = s.db.SaveMediaProbe(context.Background(), probeFromCache(itemID, sourceID, c))
	}
	return c
}

type ffprobeData struct {
	Streams []struct {
		Index            int               `json:"index"`
		CodecName        string            `json:"codec_name"`
		CodecType        string            `json:"codec_type"`
		Width            int               `json:"width"`
		Height           int               `json:"height"`
		Channels         int               `json:"channels"`
		BitDepth         int               `json:"bits_per_raw_sample"`
		PixelFormat      string            `json:"pix_fmt"`
		ColorTransfer    string            `json:"color_transfer"`
		ColorPrimaries   string            `json:"color_primaries"`
		ColorSpace       string            `json:"color_space"`
		AverageFrameRate string            `json:"avg_frame_rate"`
		Tags             map[string]string `json:"tags"`
		Disposition      map[string]int    `json:"disposition"`
	} `json:"streams"`
	Format struct {
		Size     any `json:"size"`
		Duration any `json:"duration"`
	} `json:"format"`
}

func parseProbeOutput(out []byte, c *embeddedProbeCache) error {
	var data ffprobeData
	if err := json.Unmarshal(out, &data); err != nil {
		return err
	}
	c.Size = maxInt64(c.Size, jsonInt64(data.Format.Size))
	if secs := jsonFloat64(data.Format.Duration); secs > 0 {
		c.DurationTicks = int64(secs * 10_000_000)
	}
	for _, st := range data.Streams {
		tr := embeddedSubtitle{
			FfIndex: st.Index, Type: st.CodecType, Codec: st.CodecName,
			Language: normalizeSubtitleLang(st.Tags["language"]), Title: st.Tags["title"],
			Default: st.Disposition["default"] == 1, Forced: st.Disposition["forced"] == 1,
			Width: st.Width, Height: st.Height, Channels: st.Channels, BitDepth: st.BitDepth,
			PixelFormat: st.PixelFormat, ColorTransfer: st.ColorTransfer,
			ColorPrimaries: st.ColorPrimaries, ColorSpace: st.ColorSpace,
			AverageFrameRate: parseFrameRate(st.AverageFrameRate),
		}
		if tr.Type == "subtitle" && tr.Codec == "" {
			tr.Codec = "subrip"
		}
		if tr.Type != "" {
			c.Streams = append(c.Streams, tr)
		}
	}
	return nil
}

func jsonInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	}
	return 0
}

func jsonFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return n
	}
	return 0
}

func parseFrameRate(s string) float64 {
	if i := strings.IndexByte(s, '/'); i > 0 {
		a, errA := strconv.ParseFloat(s[:i], 64)
		b, errB := strconv.ParseFloat(s[i+1:], 64)
		if errA == nil && errB == nil && b != 0 {
			return a / b
		}
	}
	n, _ := strconv.ParseFloat(s, 64)
	return n
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
