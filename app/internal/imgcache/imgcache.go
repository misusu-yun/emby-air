// Package imgcache 清理磁盘图片缓存：删除已被删除条目/媒体库的遗留图片，
// 清理过期缩略图，并给缩略图目录设置容量上限。
package imgcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Options 控制清理行为。
type Options struct {
	ServerID      string
	ThumbTTL      time.Duration // 缩略图最长保留时间，0 表示不按时间清理
	MaxThumbBytes int64         // 缩略图目录容量上限，0 表示不限制
	Now           time.Time
	// ItemExists/ViewExists 返回 false 时删除对应缓存；为 nil 表示不检查孤儿。
	ItemExists func(context.Context, string) (bool, error)
	ViewExists func(context.Context, string) (bool, error)
}

// Stats 汇总一次清理。
type Stats struct {
	Scanned int
	Removed int
	Freed   int64
}

type thumbFile struct {
	path  string
	size  int64
	mtime time.Time
}

// Clean 清理 root 下的条目图片、缩略图和媒体库封面。
func Clean(ctx context.Context, root string, opt Options) (Stats, error) {
	var st Stats
	if root == "" {
		return st, errors.New("imgcache: empty root")
	}
	if opt.Now.IsZero() {
		opt.Now = time.Now()
	}
	root = filepath.Clean(root)
	exists := map[string]bool{}
	itemExists := func(id string) (bool, error) {
		if opt.ItemExists == nil {
			return true, nil
		}
		if v, ok := exists[id]; ok {
			return v, nil
		}
		v, err := opt.ItemExists(ctx, id)
		if err != nil {
			return false, err
		}
		exists[id] = v
		return v, nil
	}

	// 条目原图/缩略图：只删除条目已经不存在的文件，不按时间删除有效条目的图。
	if opt.ServerID != "" {
		if err := cleanItemDir(ctx, filepath.Join(root, opt.ServerID), itemExists, &st); err != nil {
			return st, err
		}
	}
	// 媒体库封面：只删除已不存在的库。
	if opt.ViewExists != nil {
		if err := cleanLibraryDir(ctx, filepath.Join(root, "libraries"), opt.ViewExists, &st); err != nil {
			return st, err
		}
	}
	// 缩略图：孤儿、过期、超出容量三种都清理。
	thumbs, err := cleanThumbDir(ctx, filepath.Join(root, "thumbs"), opt, itemExists, &st)
	if err != nil {
		return st, err
	}
	if opt.MaxThumbBytes > 0 {
		var total int64
		for _, f := range thumbs {
			total += f.size
		}
		if total > opt.MaxThumbBytes {
			sort.Slice(thumbs, func(i, j int) bool { return thumbs[i].mtime.Before(thumbs[j].mtime) })
			for _, f := range thumbs {
				if total <= opt.MaxThumbBytes {
					break
				}
				if os.Remove(f.path) == nil {
					total -= f.size
					st.Removed++
					st.Freed += f.size
				}
			}
		}
	}
	return st, nil
}

func cleanItemDir(ctx context.Context, dir string, exists func(string) (bool, error), st *Stats) error {
	return walkFiles(ctx, dir, func(path string, fi os.FileInfo) (bool, error) {
		name := filepath.Base(path)
		if strings.HasSuffix(name, ".tmp") {
			return oldTemp(fi), nil
		}
		id := idPrefix(name)
		if id == "" {
			return false, nil
		}
		ok, err := exists(id)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}, st)
}

func cleanLibraryDir(ctx context.Context, dir string, exists func(context.Context, string) (bool, error), st *Stats) error {
	return walkFiles(ctx, dir, func(path string, fi os.FileInfo) (bool, error) {
		name := filepath.Base(path)
		if strings.HasSuffix(name, ".tmp") {
			return oldTemp(fi), nil
		}
		if !strings.HasSuffix(name, ".img") {
			return false, nil
		}
		id := strings.TrimSuffix(name, ".img")
		ok, err := exists(ctx, id)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}, st)
}

func cleanThumbDir(ctx context.Context, dir string, opt Options, itemExists func(string) (bool, error), st *Stats) ([]thumbFile, error) {
	var keep []thumbFile
	err := walkFiles(ctx, dir, func(path string, fi os.FileInfo) (bool, error) {
		if strings.HasSuffix(path, ".tmp") {
			return oldTemp(fi), nil
		}
		id := idPrefix(filepath.Base(path))
		if id != "" {
			ok, err := itemExists(id)
			if err != nil {
				return false, err
			}
			if !ok {
				return true, nil
			}
		}
		if opt.ThumbTTL > 0 && opt.Now.Sub(fi.ModTime()) > opt.ThumbTTL {
			return true, nil
		}
		keep = append(keep, thumbFile{path: path, size: fi.Size(), mtime: fi.ModTime()})
		return false, nil
	}, st)
	return keep, err
}

func walkFiles(ctx context.Context, dir string, decide func(string, os.FileInfo) (bool, error), st *Stats) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("imgcache: read %s: %w", dir, err)
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		st.Scanned++
		remove, err := decide(filepath.Join(dir, e.Name()), info)
		if err != nil {
			return err
		}
		if !remove {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err == nil {
			st.Removed++
			st.Freed += info.Size()
		}
	}
	return nil
}

func idPrefix(name string) string {
	i := strings.IndexByte(name, '_')
	if i <= 0 {
		return ""
	}
	return name[:i]
}

func oldTemp(fi os.FileInfo) bool {
	return time.Since(fi.ModTime()) > time.Hour
}
