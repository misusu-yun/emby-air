package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"emby-air/internal/store"
)

// storeDevice 把识别结果转成设备记录。
func storeDevice(userID string, ci ClientInfo) store.DeviceRow {
	return store.DeviceRow{
		DeviceID: ci.DeviceID, UserID: userID, Client: ci.Client,
		DeviceName: ci.Device, Version: ci.Version, Kind: ci.Kind, IP: ci.IP,
	}
}

// mediaFileExts 是判断“目录里还有媒体”的扩展名集合。
var mediaFileExts = map[string]bool{
	".strm": true, ".mkv": true, ".mp4": true, ".ts": true, ".iso": true,
	".avi": true, ".rmvb": true, ".mov": true, ".m2ts": true, ".wmv": true,
}

// handleDeleteItem 对应 DELETE /emby/Items/{Id}[?MediaSourceId=...]。
// 规则：仅管理员、仅 .strm、必须落在媒体根内、文件移入回收站、空目录才清理。
func (s *Server) handleDeleteItem(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	// API Key 是长期凭证，泄露即可删库：禁止它删除媒体。
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
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	it, err := s.db.GetItem(ctx, id)
	if err != nil || it == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	want := strings.TrimSpace(getCI(r, "MediaSourceId", "mediaSourceId"))
	// 必须用数据库里的本地路径：API 上的 MediaSource.Path 已被解析成直链。
	sources, err := s.db.LocalSources(ctx, it.Id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load sources")
		return
	}
	targets := make([]store.LocalSource, 0, len(sources))
	for _, src := range sources {
		if want != "" && src.ID != want {
			continue
		}
		if strings.TrimSpace(src.Path) != "" {
			targets = append(targets, src)
		}
	}
	if len(targets) == 0 && it.Path != "" {
		targets = append(targets, store.LocalSource{ID: it.Id, Path: it.Path})
	}
	if len(targets) == 0 {
		writeError(w, http.StatusNotFound, "没有可删除的片源")
		return
	}

	var deletedIDs []string
	var deletedPaths []string
	for _, tgt := range targets {
		ok, derr := s.deleteStrm(ctx, tgt.Path)
		if derr != nil {
			s.recordActivity(r, u.Id, u.Name, "delete_failed", it.Id, it.Name, derr.Error())
			writeError(w, http.StatusBadRequest, derr.Error())
			return
		}
		if ok {
			deletedIDs = append(deletedIDs, tgt.ID)
			deletedPaths = append(deletedPaths, tgt.Path)
		}
	}
	if len(deletedIDs) == 0 {
		writeError(w, http.StatusNotFound, "片源文件不存在或未删除")
		return
	}
	if err := s.db.RemoveMediaSources(ctx, it.Id, deletedIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "数据库同步失败: "+err.Error())
		return
	}
	s.recordActivity(r, u.Id, u.Name, "delete", it.Id, it.Name,
		fmt.Sprintf(`{"files":%d,"paths":%q}`, len(deletedIDs), deletedPaths))
	s.publish(Event{Type: "library"})
	w.WriteHeader(http.StatusNoContent)
}

// deleteStrm 删除单个片源文件（默认移入回收站）。返回是否真的处理了文件。
func (s *Server) deleteStrm(ctx context.Context, p string) (bool, error) {
	root, err := filepath.EvalSymlinks(s.cfg.MediaRoot)
	if err != nil {
		return false, fmt.Errorf("媒体根不可用: %w", err)
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return false, err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // 幂等：文件已不存在
		}
		return false, err
	}
	// 1) 白名单：必须在媒体根内，且不能是根本身
	if real == root || !strings.HasPrefix(real, root+string(os.PathSeparator)) {
		return false, fmt.Errorf("路径不在媒体库白名单内")
	}
	// 2) 只允许删除 .strm
	if !strings.EqualFold(filepath.Ext(real), ".strm") {
		return false, fmt.Errorf("只允许删除 .strm 片源")
	}
	// 3) 多条目共用同一文件：只解除引用，不落盘删除
	n, err := s.db.CountMediaSourcePath(ctx, p)
	if err == nil && n > 1 {
		return true, nil
	}
	// 4) 回收站优先
	if trash := s.trashDir(); trash != "" {
		if err := moveToTrash(trash, root, real); err != nil {
			return false, fmt.Errorf("移入回收站失败: %w", err)
		}
	} else if err := os.Remove(real); err != nil {
		return false, err
	}
	pruneEmptyDirs(filepath.Dir(real), root, 3)
	return true, nil
}

func (s *Server) trashDir() string {
	// 默认开启；媒体根不可写时 moveToTrash 会返回错误，由调用方报错。
	return filepath.Join(s.cfg.MediaRoot, ".trash")
}

// moveToTrash 把文件移动到 <root>/.trash/<yyyymmdd>/<相对路径>，保留目录结构。
func moveToTrash(trashRoot, mediaRoot, real string) error {
	rel, err := filepath.Rel(mediaRoot, real)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("路径不在媒体根内")
	}
	dst := filepath.Join(trashRoot, time.Now().Format("20060102"), rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(real, dst)
}

// pruneEmptyDirs 从 dir 开始向上清理空目录，最多 maxDepth 层，绝不删到 root。
func pruneEmptyDirs(dir, root string, maxDepth int) {
	for i := 0; i < maxDepth; i++ {
		if dir == root || !strings.HasPrefix(dir, root+string(os.PathSeparator)) {
			return
		}
		ents, err := os.ReadDir(dir)
		if err != nil || len(ents) > 0 {
			return // 目录里还有任何东西（含 .nfo/图片）就保留
		}
		if os.Remove(dir) != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}
