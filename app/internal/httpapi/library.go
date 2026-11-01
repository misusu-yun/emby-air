package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"emby-air/internal/model"
	"emby-air/internal/scanner"
)

// handleLibraryRefresh 对应 POST /emby/Library/Refresh：异步启动一次目录扫描（只扫描）。
// Emby 语义是先返回 204 再干活；并发触发时返回 409，避免两次 walk 抢同一个库。
// 注意：这里只调 RefreshAll（扫描＋入库），不再用 RefreshAllScrape —— 后者会在持 heavyMu
// 期间做全量刮削（长时间写库），Web 管理页/客户端请求会被一起拖慢。刮削只由显式任务
// （任务队列的 scrape）触发。
func (s *Server) handleLibraryRefresh(w http.ResponseWriter, r *http.Request) {
	if s.db == nil || s.cfg.MediaRoot == "" {
		writeError(w, http.StatusServiceUnavailable, "media root is not configured")
		return
	}
	if s.cfg.RefreshAll != nil {
		if !s.cfg.RefreshAll() {
			writeError(w, http.StatusConflict, "scan already running")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.StartScan(s.cfg.MediaRoot) {
		writeError(w, http.StatusConflict, "scan already running")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleVirtualFolders 对应 GET /emby/Library/VirtualFolders。
// 单库模型：一个 MediaRoot 对应一个媒体库；字段结构与 Emby 对齐，客户端只读不写。
func (s *Server) handleVirtualFolders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.db == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list libraries")
		return
	}
	views = s.filterAllowedViews(r.Context(), userFrom(r), views)
	out := make([]map[string]any, 0, len(views))
	for _, v := range views {
		out = append(out, map[string]any{
			"Name":               v.Name,
			"Locations":          viewPaths(v, s.cfg.MediaRoot),
			"CollectionType":     v.CollectionType,
			"LibraryOptions":     map[string]any{},
			"ItemId":             v.Id,
			"PrimaryImageItemId": v.Id,
			"RefreshStatus":      s.scanStatus(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleVirtualFolderAdd 对应 POST /emby/Library/VirtualFolders。
// 只登记库名与类型（路径固定为 MediaRoot），登记后立刻触发一次扫描。
// 这是“起服务后第一次能出内容”的入口，管理员专用。
func (s *Server) handleVirtualFolderAdd(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	var body struct {
		Name           string   `json:"Name"`
		CollectionType string   `json:"CollectionType"`
		Paths          []string `json:"Paths"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = "Media"
	}
	ct := strings.TrimSpace(body.CollectionType)
	if ct == "" {
		ct = "movies"
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list libraries")
		return
	}
	paths := body.Paths
	if len(paths) == 0 {
		paths = []string{s.cfg.MediaRoot}
	}
	view := model.View{
		Id:             scanner.ItemID("library/" + strings.ToLower(name)),
		Name:           name,
		CollectionType: ct,
		SortOrder:      len(views),
		Paths:          paths,
	}
	if err := s.db.UpsertViews(r.Context(), []model.View{view}); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot create library")
		return
	}
	s.invalidateLibraryStats()
	// 新建媒体库后的首次扫描走任务队列：任务中心可见、日志有进度。
	if s.cfg.EnqueueLibraryScan != nil && s.cfg.EnqueueLibraryScan(view.Id, name, "full") {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.cfg.RefreshAll != nil {
		s.cfg.RefreshAll()
	} else {
		s.StartScan(s.cfg.MediaRoot)
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleLibraryPaths 对应 GET /emby/Library/VirtualFolders/Paths（只读诊断）。
// 返回 MediaRoot 及其一级子目录，部署时用来确认挂载是否生效。
func (s *Server) handleLibraryPaths(w http.ResponseWriter, r *http.Request) {
	root := s.cfg.MediaRoot
	st, err := os.Stat(root)
	if err != nil || !st.IsDir() {
		writeError(w, http.StatusNotFound, "media root is not a directory")
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot read media root")
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	writeJSON(w, http.StatusOK, map[string]any{
		"Path":         filepath.Clean(root),
		"Children":     names,
		"IsCollection": false,
	})
}

// decodeJSONBody 解码请求体；空体不算错误（客户端常发空对象）。
func decodeJSONBody(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(http.MaxBytesReader(http.ResponseWriter(nil), r.Body, 1<<20))
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// handleMediaFolders 对应 GET /emby/Library/MediaFolders。
// 很多第三方工具（含 pansave）用它列出可用的媒体库。
func (s *Server) handleMediaFolders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.db == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Items": []any{}, "TotalRecordCount": 0})
		return
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list libraries")
		return
	}
	views = s.filterAllowedViews(r.Context(), userFrom(r), views)
	items := make([]model.Item, 0, len(views))
	for _, v := range views {
		it := viewToItem(v, s.serverID())
		it.CollectionType = v.CollectionType
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"Items":            items,
		"TotalRecordCount": len(items),
		"StartIndex":       0,
	})
}

// handleSelectableMediaFolders 对应 GET /emby/Library/SelectableMediaFolders。
func (s *Server) handleSelectableMediaFolders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.db == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list libraries")
		return
	}
	views = s.filterAllowedViews(r.Context(), userFrom(r), views)
	out := make([]map[string]any, 0, len(views))
	for _, v := range views {
		out = append(out, map[string]any{
			"Id":             v.Id,
			"Name":           v.Name,
			"CollectionType": v.CollectionType,
			"Path":           firstPath(v),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSystemConfiguration 对应 GET /emby/System/Configuration（管理端读取）。
func (s *Server) handleSystemConfiguration(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ServerName":                s.cfg.ServerName,
		"UICulture":                 "zh-CN",
		"MetadataCountryCode":       "CN",
		"PreferredMetadataLanguage": "zh-CN",
	})
}

func firstPath(v model.View) string {
	if len(v.Paths) > 0 {
		return v.Paths[0]
	}
	return ""
}

// viewPaths 返回媒体库配置的路径；老数据没有 paths 时回落到媒体根。
func viewPaths(v model.View, mediaRoot string) []string {
	if len(v.Paths) > 0 {
		return v.Paths
	}
	return []string{mediaRoot}
}

// handleLibraryDelete 对应 DELETE /emby/Library/VirtualFolders/{Id}：
// 只删除媒体库及其数据库记录，磁盘上的媒体文件保持原样。
func (s *Server) handleLibraryDelete(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load views")
		return
	}
	found := false
	for _, v := range views {
		if v.Id == id {
			found = true
			break
		}
	}
	if !found {
		writeError(w, http.StatusNotFound, "library not found")
		return
	}
	n, err := s.db.RemoveLibrary(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot remove library")
		return
	}
	// 媒体库封面缓存随库删除；条目图片由每周缓存清理按孤儿回收。
	if s.cfg.ImageCache != "" {
		cover := filepath.Join(s.cfg.ImageCache, "libraries", id+".img")
		_ = os.Remove(cover)
		_ = os.Remove(cover + ".tmp")
	}
	s.invalidateLibraryStats()
	s.recordActivity(r, u.Id, u.Name, "library_remove", id, "", `{"items":`+strconv.Itoa(n)+`}`)
	s.publish(Event{Type: "library"})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "items": n})
}
