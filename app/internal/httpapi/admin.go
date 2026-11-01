package httpapi

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"emby-air/internal/jobs"
	"emby-air/internal/model"
)

// handleAdminJobs 对应 GET/POST /emby/Admin/Jobs。
func (s *Server) handleAdminJobs(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Jobs == nil {
		writeError(w, http.StatusServiceUnavailable, "jobs unavailable")
		return
	}
	switch r.Method {
	case http.MethodGet:
		running, queued, done := s.cfg.Jobs.Snapshot()
		writeJSON(w, http.StatusOK, map[string]any{"Running": running, "Queued": queued, "Done": done})
	case http.MethodPost:
		u := userFrom(r)
		if u == nil || !u.IsAdmin {
			writeError(w, http.StatusForbidden, "admin required")
			return
		}
		var body struct {
			Kind      string `json:"Kind"`
			Mode      string `json:"Mode"`
			LibraryId string `json:"LibraryId"`
		}
		if err := decodeJSONBody(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		kind := jobs.KindScan
		if strings.EqualFold(body.Kind, "scrape") {
			kind = jobs.KindScrape
		}
		mode := jobs.ModeIncremental
		if strings.EqualFold(body.Mode, "full") {
			mode = jobs.ModeFull
		}
		name := ""
		if body.LibraryId != "" && s.db != nil {
			if views, err := s.db.GetViews(r.Context()); err == nil {
				for _, v := range views {
					if v.Id == body.LibraryId {
						name = v.Name
					}
				}
			}
		}
		j := s.cfg.Jobs.Submit(kind, mode, body.LibraryId, name)
		writeJSON(w, http.StatusAccepted, j)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAdminLogs 对应 GET /emby/Admin/Logs?limit=100。
func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Logs == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Lines": []string{}})
		return
	}
	limit := parseIntDefault(getCI(r, "limit"), 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	writeJSON(w, http.StatusOK, map[string]any{"Lines": s.cfg.Logs.Tail(limit)})
}

// handleAdminFs 对应 GET /emby/Admin/Fs?path=/test —— 给前端做目录选择器。
func (s *Server) handleAdminFs(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	p := getCI(r, "path")
	if p == "" {
		p = s.cfg.MediaRoot
	}
	p = filepath.Clean(p)
	entries, err := os.ReadDir(p)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read directory")
		return
	}
	type dirEnt struct {
		Name string `json:"Name"`
		Path string `json:"Path"`
	}
	dirs := make([]dirEnt, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		dirs = append(dirs, dirEnt{Name: e.Name(), Path: filepath.Join(p, e.Name())})
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name < dirs[j].Name })
	parent := filepath.Dir(p)
	if p == "/" {
		parent = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{"Path": p, "Parent": parent, "Dirs": dirs})
}

// handleLibraryUpdate 对应 POST /emby/Library/VirtualFolders/{Id}（改名/路径/类型）。
func (s *Server) handleLibraryUpdate(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	var body struct {
		Name           *string   `json:"Name"`
		CollectionType *string   `json:"CollectionType"`
		Paths          *[]string `json:"Paths"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list libraries")
		return
	}
	var target *model.View
	for i := range views {
		if views[i].Id == id {
			target = &views[i]
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, "library not found")
		return
	}
	if body.Name != nil && strings.TrimSpace(*body.Name) != "" {
		target.Name = strings.TrimSpace(*body.Name)
	}
	if body.CollectionType != nil && *body.CollectionType != "" {
		target.CollectionType = *body.CollectionType
	}
	pathsChanged := false
	if body.Paths != nil && len(*body.Paths) > 0 && strings.Join(*body.Paths, ",") != strings.Join(target.Paths, ",") {
		target.Paths = *body.Paths
		pathsChanged = true
	}
	if err := s.db.UpsertViews(r.Context(), []model.View{*target}); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot update library")
		return
	}
	s.invalidateLibraryStats()
	// 路径变了才需要重扫；入队后任务中心可见。
	if pathsChanged && s.cfg.EnqueueLibraryScan != nil {
		s.cfg.EnqueueLibraryScan(target.Id, target.Name, "incremental")
	}
	writeJSON(w, http.StatusOK, target)
}

// handleLibraryCover 对应 POST/GET /emby/Library/VirtualFolders/{Id}/Image。
func (s *Server) handleLibraryCover(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	dir := filepath.Join(s.cfg.ImageCache, "libraries")
	p := filepath.Join(dir, id+".img")
	if r.Method == http.MethodPost {
		u := userFrom(r)
		if u == nil || !u.IsAdmin {
			writeError(w, http.StatusForbidden, "admin required")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil || len(body) == 0 {
			writeError(w, http.StatusBadRequest, "empty image")
			return
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot store")
			return
		}
		if err := os.WriteFile(p+".tmp", body, 0o644); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot write")
			return
		}
		_ = os.Rename(p+".tmp", p)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, ok := readFileLimited(p)
	if !ok {
		// 没有封面时不要长缓存占位图：否则上传后浏览器仍显示旧图。
		w.Header().Set("Cache-Control", "no-store")
		writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
		return
	}
	sum := sha1.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.Header().Set("Content-Type", http.DetectContentType(body))
	// 必须每次回源校验：封面随时可能被替换，长缓存会导致“上传后没变化”。
	w.Header().Set("Cache-Control", "no-cache")
	if im := r.Header.Get("If-None-Match"); im != "" && ifNoneMatch(im, w.Header().Get("ETag")) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = w.Write(body)
}

// handleRuntimeStats 对应 GET /webui/api/debug/runtime（仅管理员）。
// 返回进程堆/栈/系统内存、GC 次数、goroutine 数与运行时长，
// 用于部署后确认“扫描期间内存是否回落、goroutine 是否泄漏”。
func (s *Server) handleRuntimeStats(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var uptime int64
	if !s.startedAt.IsZero() {
		uptime = int64(time.Since(s.startedAt).Seconds())
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"HeapAlloc":     ms.HeapAlloc,
		"HeapInuse":     ms.HeapInuse,
		"HeapSys":       ms.HeapSys,
		"StackInuse":    ms.StackInuse,
		"Sys":           ms.Sys,
		"NumGC":         ms.NumGC,
		"Goroutines":    runtime.NumGoroutine(),
		"UptimeSeconds": uptime,
		"NumCPU":        runtime.NumCPU(),
	})
}

// handleAdminJobCancel 对应 DELETE /emby/Admin/Jobs/{Id}：取消排队中或运行中的任务。
func (s *Server) handleAdminJobCancel(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.cfg.Jobs == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	if id == "" || !s.cfg.Jobs.Cancel(id) {
		writeError(w, http.StatusNotFound, "job not found or already finished")
		return
	}
	s.recordActivity(r, u.Id, u.Name, "job_cancel", id, "", "")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// publicUsersVisibleKey 控制 /emby/Users/Public 是否返回用户列表（登录页是否显示头像/名称）。
const publicUsersVisibleKey = "public_users_visible"

// handlePublicUsersSetting 对应 GET/POST /webui/api/public-users（管理员）。
func (s *Server) handlePublicUsersSetting(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		visible := s.db.GetSetting(r.Context(), publicUsersVisibleKey) != "0"
		writeJSON(w, http.StatusOK, map[string]any{"Visible": visible})
	case http.MethodPost:
		var body struct {
			Visible bool `json:"Visible"`
		}
		if err := decodeJSONBody(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		val := "0"
		if body.Visible {
			val = "1"
		}
		if err := s.db.SetSetting(r.Context(), publicUsersVisibleKey, val); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot save setting")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"Visible": body.Visible})
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
