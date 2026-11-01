package store

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"emby-air/internal/model"
)

func jsonUnmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }

// MediaRow 是 Web「影片管理」列表的一行。
type MediaRow struct {
	ID          string
	Type        string
	Name        string
	Year        int
	Overview    string
	Path        string
	ImageRef    string
	ScrapedAt   int64
	ScrapeState string
	Size        int64 // "" | ok | notfound | failed
}

// SearchMedia 按 中文/英文/全拼/首字母 搜索电影与剧集。
func (d *DB) SearchMedia(ctx context.Context, term string, limit, offset int) ([]MediaRow, int, error) {
	return d.SearchMediaInLibrary(ctx, term, "", limit, offset)
}

// LibraryScope 描述一个媒体库的过滤条件。
type LibraryScope struct {
	Kinds    []string
	Prefixes []string
	// ParentID 是该媒体库的 view id：条目按 parent_id 归属，统计/列表用它过滤。
	ParentID string
}

// LibraryScopeFor 按媒体库类型与路径生成过滤条件。
// 旧数据里 parent_id 可能指向错误的库（两个库都挂在 /test），所以优先用 类型 + 路径前缀。
func (d *DB) LibraryScopeFor(ctx context.Context, libID string) (LibraryScope, error) {
	views, err := d.GetViews(ctx)
	if err != nil {
		return LibraryScope{}, err
	}
	for _, v := range views {
		if v.Id != libID {
			continue
		}
		scope := LibraryScope{Prefixes: v.Paths, ParentID: v.Id}
		switch v.CollectionType {
		case "tvshows":
			// 列表只展示剧（Series）；Season/Episode 属于剧集内部层级，
			// 平铺出来就会出现“第一季”“第 1 集”混在媒体库列表里。
			scope.Kinds = []string{"Series"}
		default:
			scope.Kinds = []string{"Movie"}
		}
		return scope, nil
	}
	return LibraryScope{}, nil
}

// SearchMediaInLibrary 在指定媒体库内搜索；libraryID 为空表示全库。
// 带搜索词时全部在内存索引里过滤/分页，只回表取当前页（10 条），
// 避免 75k 行上的 LIKE 全表扫描和上千个占位符的 IN 查询。
func (d *DB) SearchMediaInLibrary(ctx context.Context, term, libraryID string, limit, offset int) ([]MediaRow, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	if strings.TrimSpace(term) != "" {
		scope := LibraryScope{Kinds: []string{"Movie", "Series"}}
		var validParents map[string]bool
		if libraryID != "" {
			scope, _ = d.LibraryScopeFor(ctx, libraryID)
			if len(scope.Kinds) == 0 {
				scope = LibraryScope{Kinds: []string{"Movie", "Series"}}
			}
		} else if views, err := d.GetViews(ctx); err == nil {
			// 全库搜索也只返回已添加媒体库里的条目。
			validParents = make(map[string]bool, len(views))
			for _, v := range views {
				validParents[v.Id] = true
			}
		}
		entries := d.SearchEntries(ctx, term)
		filtered := entries[:0]
		legacy := make([]searchEntry, 0, 8)
		for _, e := range entries {
			if !inScope(e, scope) {
				continue
			}
			if validParents != nil && !validParents[e.parent] {
				legacy = append(legacy, e) // parent 为空的历史数据，作为兜底
				continue
			}
			filtered = append(filtered, e)
		}
		if len(filtered) == 0 && !d.HasParentedEntries(ctx) {
			// 只有旧库/测试夹具里存在完全未归属的数据时才允许这种兜底。
			if validParents != nil {
				filtered = append(filtered, legacy...)
			} else {
				for _, e := range entries {
					kindOK := len(scope.Kinds) == 0
					for _, k := range scope.Kinds {
						if e.kind == k {
							kindOK = true
							break
						}
					}
					if kindOK {
						filtered = append(filtered, e)
					}
				}
			}
		}
		sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].sortName < filtered[j].sortName })
		total := len(filtered)
		if offset >= total {
			return []MediaRow{}, total, nil
		}
		endIdx := offset + limit
		if endIdx > total {
			endIdx = total
		}
		ids := make([]string, 0, endIdx-offset)
		for _, e := range filtered[offset:endIdx] {
			ids = append(ids, e.id)
		}
		rows, err := d.mediaRowsByIDs(ctx, ids)
		if err != nil {
			return nil, 0, err
		}
		return orderRows(rows, ids), total, nil
	}

	kinds := []string{"Movie", "Series"}
	scope := LibraryScope{}
	scoped := false
	var parentIDs []string
	if libraryID != "" {
		if sc, err := d.LibraryScopeFor(ctx, libraryID); err == nil && len(sc.Kinds) > 0 {
			kinds = sc.Kinds
			scope = sc
			scoped = true
		}
	} else if views, err := d.GetViews(ctx); err == nil {
		for _, v := range views {
			parentIDs = append(parentIDs, v.Id)
		}
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(kinds)), ",")
	// buildWhere 生成过滤条件：正常走 parent_id 索引；
	// legacy 数据（parent 为空）在首次查询为 0 时回退到路径前缀。
	buildWhere := func(byPath bool) (string, []any) {
		where := "i.kind IN (" + ph + ")"
		args := make([]any, 0, len(kinds)+2)
		for _, k := range kinds {
			args = append(args, k)
		}
		if !scoped {
			// 未选库（“全部”）也只列已添加媒体库中的条目。
			// 用显式占位符而不是 IN (SELECT ...)，避免优化器退化成全表扫。
			if len(parentIDs) == 0 {
				where += " AND 0=1"
			} else {
				ph := strings.TrimSuffix(strings.Repeat("?,", len(parentIDs)), ",")
				where += " AND i.parent_id IN (" + ph + ")"
				for _, id := range parentIDs {
					args = append(args, id)
				}
			}
		}
		if scoped {
			if byPath && len(scope.Prefixes) > 0 {
				var ors []string
				for _, p := range scope.Prefixes {
					ors = append(ors, "i.path LIKE ?")
					args = append(args, strings.TrimRight(p, "/")+"%")
				}
				where += " AND (" + strings.Join(ors, " OR ") + ")"
			} else {
				where += " AND i.parent_id = ?"
				args = append(args, scope.ParentID)
			}
		}
		return where, args
	}
	where, args := buildWhere(false)
	var total int
	if err := d.sql.QueryRowContext(ctx,
		`SELECT count(*) FROM items i WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	if total == 0 && scoped && len(scope.Prefixes) > 0 {
		where, args = buildWhere(true)
		if err := d.sql.QueryRowContext(ctx,
			`SELECT count(*) FROM items i WHERE `+where, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	// 两段式：先用覆盖索引只取本页 10 个 ID（不读宽字段），再回表取这 10 行。
	idSQL := `SELECT i.id FROM items i WHERE ` + where + ` ORDER BY i.sort_name, i.id LIMIT ? OFFSET ?`
	idRows, err := d.sql.QueryContext(ctx, idSQL, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]string, 0, limit)
	for idRows.Next() {
		var id string
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return nil, 0, err
		}
		ids = append(ids, id)
	}
	idRows.Close()
	if err := idRows.Err(); err != nil {
		return nil, 0, err
	}
	if len(ids) == 0 {
		return []MediaRow{}, total, nil
	}
	rows, err := d.mediaRowsByIDs(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	return orderRows(rows, ids), total, nil
}

// mediaRowsByIDs 取一页条目的完整信息（含刮削状态）。
func (d *DB) mediaRowsByIDs(ctx context.Context, ids []string) ([]MediaRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := d.sql.QueryContext(ctx, `SELECT i.id, i.kind, i.name, coalesce(i.year,0), coalesce(i.overview,''),
		coalesce(i.path,''), coalesce(i.image_ref,''), coalesce(i.scraped_at,0), coalesce(s.status,''),
		coalesce((SELECT max(size) FROM media_sources ms WHERE ms.item_id=i.id),0)
		FROM items i LEFT JOIN scrape_state s ON s.item_id=i.id WHERE i.id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]MediaRow, 0, len(ids))
	for rows.Next() {
		var r MediaRow
		if err := rows.Scan(&r.ID, &r.Type, &r.Name, &r.Year, &r.Overview, &r.Path,
			&r.ImageRef, &r.ScrapedAt, &r.ScrapeState, &r.Size); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func orderRows(rows []MediaRow, ids []string) []MediaRow {
	idx := make(map[string]int, len(ids))
	for i, id := range ids {
		idx[id] = i
	}
	sort.SliceStable(rows, func(i, j int) bool { return idx[rows[i].ID] < idx[rows[j].ID] })
	return rows
}

// MediaLibraryStats 是“影片”栏目顶部的媒体库统计。
type MediaLibraryStats struct {
	Id           string `json:"Id"`
	Name         string `json:"Name"`
	Type         string `json:"Type"`
	MovieCount   int    `json:"MovieCount"`
	SeriesCount  int    `json:"SeriesCount"`
	EpisodeCount int    `json:"EpisodeCount"`
	SortOrder    int    `json:"SortOrder"`
}

// LibraryStats 统计每个媒体库的电影/剧集/单集数量。
// 全部走索引：kind+path 前缀，单集用 series_id 子查询，避免大表 JOIN + OR 的 30 秒全表扫。
func (d *DB) LibraryStats(ctx context.Context) ([]MediaLibraryStats, error) {
	views, err := d.GetViews(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MediaLibraryStats, 0, len(views))
	for _, v := range views {
		st := MediaLibraryStats{Id: v.Id, Name: v.Name, Type: v.CollectionType, SortOrder: v.SortOrder}
		pathWhere := func() (string, []any) {
			if len(v.Paths) == 0 {
				return "", nil
			}
			ors := make([]string, 0, len(v.Paths))
			args := make([]any, 0, len(v.Paths))
			for _, p := range v.Paths {
				if p = strings.TrimRight(strings.TrimSpace(p), "/"); p != "" {
					ors = append(ors, "path LIKE ?")
					args = append(args, p+"%")
				}
			}
			return strings.Join(ors, " OR "), args
		}
		countKind := func(kind string) int {
			var n int
			// 按 parent_id 统计：库归属已由 NormalizeLibraryParents 按路径修正，
			// 一个库配多个路径时也能全部计入（旧实现只看 Paths[0]）。
			_ = d.sql.QueryRowContext(ctx,
				`SELECT count(*) FROM items WHERE kind=? AND parent_id=?`, kind, v.Id).Scan(&n)
			if n == 0 {
				if pw, pa := pathWhere(); pw != "" {
					_ = d.sql.QueryRowContext(ctx,
						`SELECT count(*) FROM items WHERE kind=? AND (`+pw+`)`,
						append([]any{kind}, pa...)...).Scan(&n)
				}
			}
			return n
		}
		switch v.CollectionType {
		case "tvshows":
			st.SeriesCount = countKind("Series")
			var n int
			// 强制走 (series_id, kind) 覆盖索引：默认计划会按 kind 扫 7 万行再回表。
			// CROSS JOIN 固定连接顺序：先按 parent 索引取该库剧集，再用
			// (series_id,kind) 覆盖索引数单集；默认计划会去扫 7 万行 Episode。
			_ = d.sql.QueryRowContext(ctx, `SELECT count(*) FROM items s
				CROSS JOIN items e INDEXED BY idx_items_series_kind
				WHERE s.kind='Series' AND s.parent_id=? AND e.series_id=s.id AND e.kind='Episode'`, v.Id).Scan(&n)
			if n == 0 {
				if pw, pa := pathWhere(); pw != "" {
					_ = d.sql.QueryRowContext(ctx, `SELECT count(*) FROM items e INDEXED BY idx_items_series_kind
						WHERE e.kind='Episode' AND e.series_id IN (
							SELECT id FROM items WHERE kind='Series' AND (`+pw+`))`,
						pa...).Scan(&n)
				}
			}
			st.EpisodeCount = n
		default:
			st.MovieCount = countKind("Movie")
		}
		out = append(out, st)
	}
	return out, nil
}

// SetLibraryOrder 把某媒体库移动到指定序号（1 基），其余库顺序自动顺延。
func (d *DB) SetLibraryOrder(ctx context.Context, id string, position int) error {
	views, err := d.GetViews(ctx)
	if err != nil {
		return err
	}
	if position < 1 {
		position = 1
	}
	if position > len(views) {
		position = len(views)
	}
	ordered := make([]model.View, 0, len(views))
	var moving *model.View
	for i := range views {
		if views[i].Id == id {
			v := views[i]
			moving = &v
			continue
		}
		ordered = append(ordered, views[i])
	}
	if moving == nil {
		return nil
	}
	pos := position - 1
	ordered = append(ordered, model.View{})
	copy(ordered[pos+1:], ordered[pos:])
	ordered[pos] = *moving

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := range ordered {
		if _, err := tx.ExecContext(ctx, `UPDATE views SET sort_order=? WHERE id=?`, i+1, ordered[i].Id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ItemByID 返回条目的基础信息（供管理接口判断类型/路径）。
func (d *DB) ItemRow(ctx context.Context, id string) (*MediaRow, error) {
	var r MediaRow
	err := d.sql.QueryRowContext(ctx, `SELECT id, kind, name, coalesce(year,0), coalesce(overview,''),
		coalesce(path,''), coalesce(image_ref,''), coalesce(scraped_at,0)
		FROM items WHERE id=?`, id).
		Scan(&r.ID, &r.Type, &r.Name, &r.Year, &r.Overview, &r.Path, &r.ImageRef, &r.ScrapedAt)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// DeleteItemTree 删除条目及其所有子条目（剧集→季→集）与关联数据。
// 返回被删除的条目 ID（调用方据此清理图片缓存）。
func (d *DB) DeleteItemTree(ctx context.Context, id string) ([]string, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var kind, path string
	if err := tx.QueryRowContext(ctx, `SELECT kind, coalesce(path,'') FROM items WHERE id=?`, id).
		Scan(&kind, &path); err != nil {
		return nil, err
	}

	ids := []string{id}
	if kind == "Series" {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM items WHERE series_id=? OR parent_id=?`, id, id)
		if err == nil {
			for rows.Next() {
				var cid string
				if err := rows.Scan(&cid); err == nil {
					ids = append(ids, cid)
				}
			}
			rows.Close()
		}
		// 季：parent_id 指向剧集
		rows2, err := tx.QueryContext(ctx, `SELECT id FROM items WHERE parent_id IN (
			SELECT id FROM items WHERE parent_id=?)`, id)
		if err == nil {
			for rows2.Next() {
				var cid string
				if err := rows2.Scan(&cid); err == nil {
					ids = append(ids, cid)
				}
			}
			rows2.Close()
		}
	}

	for _, iid := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=?`, iid); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM user_data WHERE item_id=?`, iid); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM scrape_state WHERE item_id=?`, iid); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, iid); err != nil {
			return nil, err
		}
	}
	// 标题目录下的路径记录（按目录前缀清理）
	if dir := mediaDirOf(path); dir != "" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM item_paths WHERE path LIKE ?`, dir+"%"); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

// mediaDirOf 返回视频文件的上一级目录（不带结尾分隔符）。
func mediaDirOf(p string) string {
	i := strings.LastIndexAny(p, `/\`)
	if i <= 0 {
		return ""
	}
	return p[:i]
}

// ScrapeCandidateByID 取单个条目的刮削候选（字段与 ListScrapeCandidates 一致）。
func (d *DB) ScrapeCandidateByID(ctx context.Context, id string) (*ScrapeCandidate, error) {
	hint := `CASE WHEN i.kind='Episode' THEN COALESCE(parent.provider_ids,'{}') ELSE '{}' END`
	hasEpInfo := `CASE WHEN i.kind='Episode' THEN
		CASE WHEN COALESCE(i.episode_number,0)>0 AND COALESCE(i.season_number,0)>=0 THEN 1 ELSE 0 END
		ELSE 1 END`
	var c ScrapeCandidate
	var hintJSON, provJSON string
	var hasOverview, epInfo int
	err := d.sql.QueryRowContext(ctx, `SELECT i.id, i.kind, i.name, COALESCE(i.year,0), COALESCE(i.path,''),
		i.parent_id, COALESCE(i.series_name,''), `+hint+`, `+hasEpInfo+`,
		COALESCE(i.season_number,0), COALESCE(i.episode_number,0),
		COALESCE(i.provider_ids,'{}'),
		CASE WHEN COALESCE(i.overview,'')<>'' THEN 1 ELSE 0 END
		FROM items i LEFT JOIN items parent ON parent.id = i.series_id
		WHERE i.id=?`, id).
		Scan(&c.Id, &c.Type, &c.Name, &c.Year, &c.Path, &c.ParentId, &c.SeriesName,
			&hintJSON, &epInfo, &c.SeasonNum, &c.EpisodeNum, &provJSON, &hasOverview)
	if err != nil {
		return nil, err
	}
	_ = jsonUnmarshal(provJSON, &c.ProviderIds)
	if c.Type == "Episode" && hintJSON != "" {
		var m map[string]string
		_ = jsonUnmarshal(hintJSON, &m)
		c.SeriesTmdb = m["tmdb"]
	}
	c.HasOverview = hasOverview == 1
	c.HasEpisodeInfo = epInfo == 1
	return &c, nil
}
