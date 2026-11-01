package store

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// 内存搜索索引：大表上 `LIKE '%x%'` 要全表扫三列（线上 10~20 秒）。
// 只索引电影/剧集：单集数量是前者的十倍以上，而客户端搜索/媒体库列表都按剧集层级展示。
type searchEntry struct {
	id       string
	name     string // 小写
	pinyin   string
	initials string
	kind     string
	parent   string
	sortName string
	// 不再缓存 path：15 万条的长路径字符串占用大量常驻内存。
}

type searchIndex struct {
	mu      sync.RWMutex
	entries []searchEntry
	loaded  bool
	// hasParented 表示索引里存在已归属媒体库的条目（真实数据）。
	// 全为空只出现在旧库/测试夹具，这时搜索才允许退回未归属数据。
	hasParented bool
}

var (
	searchDirty  atomic.Bool
	searchPaused atomic.Bool
)

func markSearchDirty() { searchDirty.Store(true) }

// SetSearchPaused 在扫描期间暂停索引重建：写库会持续把索引标脏，
// 如果此时有 Web 搜索请求，就会反复全表加载 7 万行，造成内存尖峰。
func SetSearchPaused(p bool) { searchPaused.Store(p) }

// searchIndex 绑定在 DB 上（每个进程一个）。
func (d *DB) getSearchIndex(ctx context.Context) []searchEntry {
	d.si.mu.RLock()
	if d.si.loaded && (!searchDirty.Load() || searchPaused.Load()) {
		entries := d.si.entries
		d.si.mu.RUnlock()
		return entries
	}
	d.si.mu.RUnlock()

	d.si.mu.Lock()
	defer d.si.mu.Unlock()
	if d.si.loaded && !searchDirty.Load() {
		return d.si.entries
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, lower(name), coalesce(pinyin,''), coalesce(pinyin_initials,''),
		        kind, coalesce(parent_id,''), sort_name
		 FROM items WHERE kind IN ('Movie','Series')`)
	if err != nil {
		return d.si.entries
	}
	entries := make([]searchEntry, 0, 1<<16)
	hasParented := false
	for rows.Next() {
		var e searchEntry
		if err := rows.Scan(&e.id, &e.name, &e.pinyin, &e.initials, &e.kind, &e.parent, &e.sortName); err != nil {
			break
		}
		if e.parent != "" {
			hasParented = true
		}
		entries = append(entries, e)
	}
	rows.Close()
	d.si.entries = entries
	d.si.hasParented = hasParented
	d.si.loaded = true
	searchDirty.Store(false)
	return entries
}

// SearchIDs 返回匹配 中文/拼音/首字母 的条目 ID（最多 limit 条）。
// 空查询返回 ok=false，调用方保持原有查询语义。
// SearchEntries 返回匹配的索引条目（用于 Web 管理页在内存里分页/过滤）。
func (d *DB) SearchEntries(ctx context.Context, term string) []searchEntry {
	t := strings.ToLower(strings.TrimSpace(term))
	if t == "" {
		return nil
	}
	compact := strings.ReplaceAll(t, " ", "")
	entries := d.getSearchIndex(ctx)
	out := make([]searchEntry, 0, 64)
	for _, e := range entries {
		if strings.Contains(e.name, t) ||
			(compact != "" && (strings.Contains(e.pinyin, compact) || strings.Contains(e.initials, compact))) {
			out = append(out, e)
		}
	}
	return out
}

func (d *DB) SearchIDs(ctx context.Context, term string, limit int) ([]string, bool) {
	t := strings.ToLower(strings.TrimSpace(term))
	if t == "" {
		return nil, false
	}
	compact := strings.ReplaceAll(t, " ", "")
	if limit <= 0 || limit > 5000 {
		limit = 2000
	}
	entries := d.getSearchIndex(ctx)
	out := make([]string, 0, 64)
	for _, e := range entries {
		if strings.Contains(e.name, t) || (compact != "" &&
			(strings.Contains(e.pinyin, compact) || strings.Contains(e.initials, compact))) {
			out = append(out, e.id)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, true
}

// matchCount 仅用于排序稳定（按名称长度升序更接近“精确优先”）。
func sortByRelevance(ids []string, entries map[string]searchEntry) {
	sort.SliceStable(ids, func(i, j int) bool {
		return len(entries[ids[i]].name) < len(entries[ids[j]].name)
	})
}

// inScope 判断索引条目是否属于某个媒体库（类型 + 路径前缀）。
func inScope(e searchEntry, scope LibraryScope) bool {
	kindOK := len(scope.Kinds) == 0
	for _, k := range scope.Kinds {
		if e.kind == k {
			kindOK = true
			break
		}
	}
	if !kindOK {
		return false
	}
	if scope.ParentID != "" {
		return e.parent == scope.ParentID
	}
	return true
}

// WarmSearchIndex 在后台构建/刷新搜索索引（仅在数据变化后才真正重建）。
func (d *DB) WarmSearchIndex(ctx context.Context) {
	if !searchDirty.Load() && d.si.loaded {
		return
	}
	_ = d.getSearchIndex(ctx)
}

// HasParentedEntries 表示搜索索引中是否存在已归属媒体库的条目。
func (d *DB) HasParentedEntries(ctx context.Context) bool {
	_ = d.getSearchIndex(ctx)
	d.si.mu.RLock()
	defer d.si.mu.RUnlock()
	return d.si.hasParented
}
