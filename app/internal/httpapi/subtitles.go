package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

// enrichItemSubtitles 在详情/播放信息中补外挂字幕；只读文件系统，不改数据库。
func (s *Server) enrichItemSubtitles(_ context.Context, it *model.Item) {
	if it == nil {
		return
	}
	attachExternalSubtitles(it.MediaSources, it.Id)
}
