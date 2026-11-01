package store

import (
	"context"
	"database/sql"
	"errors"
)

// ItemViewID 返回条目所属的媒体库 ID（views.id）。
// Movie/Series 的 parent_id 就是媒体库；Season 的 parent_id 是 Series；
// Episode 的 series_id 是 Series。找不到或未归属时返回空串。
// 供 HTTP 层做“普通用户只能访问授权媒体库”的校验。
func (d *DB) ItemViewID(ctx context.Context, itemID string) (string, error) {
	cur := itemID
	for i := 0; i < 6; i++ {
		if cur == "" {
			return "", nil
		}
		var kind, parent, series string
		err := d.sql.QueryRowContext(ctx,
			`SELECT kind, coalesce(parent_id,''), coalesce(series_id,'') FROM items WHERE id=?`, cur).
			Scan(&kind, &parent, &series)
		if errors.Is(err, sql.ErrNoRows) {
			// 可能是媒体库本身（views 不在 items 表里）。
			var n int
			if e := d.sql.QueryRowContext(ctx, `SELECT count(*) FROM views WHERE id=?`, cur).Scan(&n); e == nil && n > 0 {
				return cur, nil
			}
			return "", nil
		}
		if err != nil {
			return "", err
		}
		switch kind {
		case "Movie", "Series":
			if parent == "" {
				return "", nil
			}
			return parent, nil
		case "Season":
			cur = parent // 季的父级是 Series
		case "Episode":
			if series != "" {
				cur = series
			} else {
				cur = parent // 父级是 Season
			}
		default:
			if parent != "" {
				cur = parent
				continue
			}
			return "", nil
		}
	}
	return "", nil
}
