package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"emby-air/internal/scanner"
	"emby-air/internal/store"
)

// MediaItemVM 是影片管理列表的一行（含刮削状态与封面地址）。
type MediaItemVM struct {
	Id          string `json:"Id"`
	Type        string `json:"Type"`
	Name        string `json:"Name"`
	Year        int    `json:"Year"`
	Overview    string `json:"Overview"`
	Path        string `json:"Path"`
	Dir         string `json:"Dir"`
	Status      string `json:"Status"` // scraped | unscraped | notfound | failed
	StatusLabel string `json:"StatusLabel"`
	ImageUrl    string `json:"ImageUrl"`
	ScrapedAt   int64  `json:"ScrapedAt"`
	Size        int64  `json:"Size"`
}

// handleWebMediaList 对应 GET /webui/api/media?q=&limit=&offset=。
func (s *Server) handleWebMediaList(w http.ResponseWriter, r *http.Request) {
	if u := userFrom(r); u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if s.db == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Items": []any{}, "Total": 0})
		return
	}
	q := strings.TrimSpace(getCI(r, "q", "SearchTerm", "searchterm"))
	library := strings.TrimSpace(getCI(r, "library", "LibraryId"))
	limit := parseIntDefault(getCI(r, "limit"), 10)
	page := parseIntDefault(getCI(r, "page"), 1)
	if page < 1 {
		page = 1
	}
	offset := parseIntDefault(getCI(r, "offset"), (page-1)*limit)

	rows, total, err := s.db.SearchMediaInLibrary(r.Context(), q, library, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "查询失败: "+err.Error())
		return
	}
	out := make([]MediaItemVM, 0, len(rows))
	for _, m := range rows {
		vm := MediaItemVM{
			Id: m.ID, Type: m.Type, Name: m.Name, Year: m.Year,
			Overview: truncateRunes(m.Overview, 120), Path: m.Path,
			Dir: s.itemDir(r.Context(), m.Path), ScrapedAt: m.ScrapedAt, Size: m.Size,
		}
		switch {
		case m.ScrapeState == "ok" && m.ScrapedAt > 0:
			vm.Status, vm.StatusLabel = "scraped", "已刮削"
		case m.ScrapeState == "failed":
			vm.Status, vm.StatusLabel = "failed", "刮削失败"
		case m.ScrapeState == "notfound":
			vm.Status, vm.StatusLabel = "notfound", "未匹配"
		case m.ScrapedAt > 0:
			vm.Status, vm.StatusLabel = "scraped", "已刮削"
		default:
			vm.Status, vm.StatusLabel = "unscraped", "未刮削"
		}
		// 无封面也给出地址：接口会返回占位图，前端不用做分支。
		vm.ImageUrl = "/emby/Items/" + m.ID + "/Images/Primary?maxWidth=200"
		out = append(out, vm)
	}
	// 管理页数据不做浏览器缓存，避免删除库后仍看到旧条目。
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"Items": out, "Total": total, "Offset": offset, "Page": page, "Limit": limit,
	})
}

// itemDir 返回条目的标题目录（防误删的关键：绝不能返回分类目录或媒体库根）。
func (s *Server) itemDir(_ context.Context, p string) string {
	if p == "" {
		return ""
	}
	return scanner.TitleDir(p, s.cfg.MediaRoot)
}

// handleWebMediaRescan 对应 POST /webui/api/media/{Id}/rescan。
func (s *Server) handleWebMediaRescan(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	row, _ := s.db.ItemRow(r.Context(), id)
	name := id
	if row != nil {
		name = row.Name
	}
	if s.cfg.EnqueueItemJob != nil {
		if !s.cfg.EnqueueItemJob("rescan", id, name) {
			writeError(w, http.StatusServiceUnavailable, "任务队列不可用")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "queued": true})
		return
	}
	if s.cfg.RescanItem == nil {
		writeError(w, http.StatusNotImplemented, "扫描器未就绪")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	if err := s.cfg.RescanItem(ctx, id); err != nil {
		s.recordActivity(r, u.Id, u.Name, "rescan_failed", id, "", err.Error())
		writeError(w, http.StatusInternalServerError, "扫描失败: "+err.Error())
		return
	}
	s.recordActivity(r, u.Id, u.Name, "rescan", id, "", "")
	s.publish(Event{Type: "library"})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleWebMediaRescore 对应 POST /webui/api/media/{Id}/rescore。
func (s *Server) handleWebMediaRescore(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	row, _ := s.db.ItemRow(r.Context(), id)
	name := id
	if row != nil {
		name = row.Name
	}
	if s.cfg.EnqueueItemJob != nil {
		if !s.cfg.EnqueueItemJob("rescore", id, name) {
			writeError(w, http.StatusServiceUnavailable, "任务队列不可用")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "queued": true})
		return
	}
	if s.cfg.RescoreItem == nil {
		writeError(w, http.StatusNotImplemented, "刮削器未就绪")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	if err := s.cfg.RescoreItem(ctx, id); err != nil {
		s.recordActivity(r, u.Id, u.Name, "rescore_failed", id, "", err.Error())
		writeError(w, http.StatusInternalServerError, "刮削失败: "+err.Error())
		return
	}
	s.recordActivity(r, u.Id, u.Name, "rescore", id, "", "")
	s.publish(Event{Type: "library"})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleWebMediaDelete 对应 DELETE /webui/api/media/{Id}：
// 把整部影片（标题目录）移入回收站，并清理数据库与图片缓存。
func (s *Server) handleWebMediaDelete(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if isAPIKeyUser(u) {
		writeError(w, http.StatusForbidden, "API key cannot delete media")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	ctx := r.Context()
	id := r.PathValue("Id")
	row, err := s.db.ItemRow(ctx, id)
	if err != nil || row == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	dir := s.itemDir(ctx, row.Path)
	// 路径白名单：目录必须真实存在于媒体根内，且不能就是媒体根本身。
	if dir != "" {
		if err := s.assertDeletable(rootOf(s.cfg.MediaRoot), dir); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	ids, err := s.db.DeleteItemTree(ctx, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "数据库删除失败: "+err.Error())
		return
	}

	var moved string
	if dir != "" {
		moved, err = s.moveDirToTrash(dir)
		if err != nil {
			// 文件没动成功：数据库已删，仍要显式告诉调用方，避免“看起来删了但磁盘还在”。
			s.recordActivity(r, u.Id, u.Name, "delete_failed", id, row.Name, err.Error())
			writeError(w, http.StatusInternalServerError, "文件删除失败: "+err.Error())
			return
		}
	}

	// 图片缓存：<ImageCache>/<server>/<id>_*.jpg 以及季封面 <id>_sN_poster.jpg
	removed := s.purgeImageCache(ids)
	s.recordActivity(r, u.Id, u.Name, "delete", id, row.Name,
		fmt.Sprintf(`{"items":%d,"dir":%q,"images":%d}`, len(ids), moved, removed))
	s.publish(Event{Type: "library"})
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "items": len(ids), "images": removed, "trash": moved,
	})
}

// assertDeletable 校验目标目录在媒体根内且不是根本身。
func (s *Server) assertDeletable(root, dir string) error {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("媒体根不可用: %w", err)
	}
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("目录不可用: %w", err)
	}
	if realDir == realRoot {
		return fmt.Errorf("拒绝删除媒体库根目录")
	}
	if !strings.HasPrefix(realDir, realRoot+string(os.PathSeparator)) {
		return fmt.Errorf("目录不在媒体库白名单内")
	}
	return nil
}

func rootOf(p string) string { return strings.TrimRight(p, "/") }

// moveDirToTrash 把整个标题目录移到 <媒体根>/.trash/<日期>/<相对路径>。
func (s *Server) moveDirToTrash(dir string) (string, error) {
	root := rootOf(s.cfg.MediaRoot)
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	// macOS 的 /var → /private/var 是符号链接，必须先把 dir 也解析掉再求相对路径。
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(realRoot, realDir)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("路径不在媒体根内")
	}
	dst := filepath.Join(realRoot, ".trash", time.Now().Format("20060102"), rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(realDir, dst); err != nil {
		return "", err
	}
	// 顺手清掉空出来的分类目录（最多向上 3 层，绝不到达媒体根）。
	pruneEmptyDirs(filepath.Dir(realDir), realRoot, 3)
	return dst, nil
}

// purgeImageCache 删除这些条目对应的缓存图片（原图、剧照、缩略图）。
func (s *Server) purgeImageCache(ids []string) int {
	if s.cfg.ImageCache == "" {
		return 0
	}
	dirs := []string{
		filepath.Join(s.cfg.ImageCache, s.serverID()),
		filepath.Join(s.cfg.ImageCache, "thumbs"),
	}
	n := 0
	for _, id := range ids {
		for _, dir := range dirs {
			matches, _ := filepath.Glob(filepath.Join(dir, id+"_*"))
			for _, f := range matches {
				if os.Remove(f) == nil {
					n++
				}
			}
		}
	}
	return n
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// handleWebLibraries 返回媒体库列表与电影/剧集/单集数量（“影片”栏目顶部按钮）。
func (s *Server) handleWebLibraries(w http.ResponseWriter, r *http.Request) {
	if u := userFrom(r); u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if s.db == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	stats, err := s.libraryStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "查询失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleWebLibraryOrder 调整媒体库顺序：
// POST /webui/api/libraries/{Id}/order {"Position": 2}  或  {"Direction": "up"|"down"}
func (s *Server) handleWebLibraryOrder(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	id := r.PathValue("Id")
	var body struct {
		Position  int    `json:"Position"`
		Direction string `json:"Direction"`
	}
	_ = decodeJSONBody(r, &body)

	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load views")
		return
	}
	pos := body.Position
	if pos == 0 && body.Direction != "" {
		for i, v := range views {
			if v.Id != id {
				continue
			}
			if body.Direction == "up" {
				pos = i
			} else {
				pos = i + 2
			}
			break
		}
	}
	if pos < 1 {
		pos = 1
	}
	if err := s.db.SetLibraryOrder(r.Context(), id, pos); err != nil {
		writeError(w, http.StatusInternalServerError, "排序失败: "+err.Error())
		return
	}
	s.recordActivity(r, u.Id, u.Name, "library_order", id, "", `{"position":`+fmt.Sprint(pos)+`}`)
	s.publish(Event{Type: "library"})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "position": pos})
}

// handleWebMediaSize 探测 STRM 源文件大小（HTTP HEAD Content-Length），结果缓存到 media_sources.size。
// 只在用户点击时触发；单源 4 秒超时，最多处理 5 个源。
func (s *Server) handleWebMediaSize(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	srcs, err := s.db.LocalSources(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load sources")
		return
	}
	type result struct {
		Id   string `json:"Id"`
		Size int64  `json:"Size"`
	}
	out := make([]result, 0, len(srcs))
	if len(srcs) > 5 {
		srcs = srcs[:5]
	}
	client := &http.Client{Timeout: 4 * time.Second}
	for _, src := range srcs {
		target := src.Path
		if strings.EqualFold(filepath.Ext(target), ".strm") {
			if b, err := os.ReadFile(target); err == nil {
				for _, line := range strings.Split(string(b), "\n") {
					if v := strings.TrimSpace(line); v != "" && !strings.HasPrefix(v, "#") {
						target = v
						break
					}
				}
			}
		}
		size := int64(0)
		if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
			if req, err := http.NewRequestWithContext(r.Context(), http.MethodHead, target, nil); err == nil {
				if res, err := client.Do(req); err == nil {
					size = res.ContentLength
					res.Body.Close()
				}
			}
		}
		if size > 0 {
			_ = s.db.SetMediaSourceSize(r.Context(), id, src.ID, size)
		}
		out = append(out, result{Id: src.ID, Size: size})
	}
	writeJSON(w, http.StatusOK, out)
}

const libraryStatsTTL = 5 * time.Minute

// libraryStats 缓存媒体库统计。单集计数需要遍历分集，HDD 上单次要数秒；
// 过期后先用旧值响应，再后台刷新（stale-while-revalidate），保证切库永不阻塞。
func (s *Server) libraryStats(ctx context.Context) ([]store.MediaLibraryStats, error) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if s.statsData != nil {
		data := s.statsData
		if time.Since(s.statsAt) >= libraryStatsTTL && !s.statsRefreshing {
			s.statsRefreshing = true
			s.startBackground(s.refreshLibraryStats)
		}
		return data, nil
	}
	// 首次没有缓存：持锁计算，后续请求排队等待同一份结果，避免并发重复算。
	data, err := s.db.LibraryStats(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		s.statsData, s.statsAt = data, time.Now()
	}
	return data, nil
}

func (s *Server) refreshLibraryStats() {
	ctx := s.lifeCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	data, err := s.db.LibraryStats(ctx)
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.statsRefreshing = false
	if err == nil && len(data) > 0 {
		s.statsData, s.statsAt = data, time.Now()
	}
}

// invalidateLibraryStats 在媒体库增删改后调用，避免 30 秒内显示旧统计。
func (s *Server) invalidateLibraryStats() {
	s.statsMu.Lock()
	s.statsData = nil
	s.statsAt = time.Time{}
	s.statsMu.Unlock()
}

// warmLibraryStats 启动后后台算一次，避免用户第一次点开影片管理时等待。
func (s *Server) warmLibraryStats() {
	if s.db == nil {
		return
	}
	s.startBackground(func() {
		ctx := s.lifeCtx
		if ctx == nil {
			ctx = context.Background()
		}
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		_, _ = s.libraryStats(ctx)
	})
}
