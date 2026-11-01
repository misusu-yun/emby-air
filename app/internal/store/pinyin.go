package store

import (
	"context"
	"strings"

	"github.com/mozillazg/go-pinyin"
)

// nameIndexes 生成标题的拼音索引：全拼（无分隔、小写）与首字母。
// 非中文字符（英文/数字）原样保留，便于中英混排标题也能被搜到。
func nameIndexes(name string) (full, initials string) {
	args := pinyin.NewArgs()
	args.Style = pinyin.Normal
	args.Heteronym = false

	var fb, ib strings.Builder
	for _, ch := range name {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' {
			l := strings.ToLower(string(ch))
			fb.WriteString(l)
			ib.WriteString(l)
			continue
		}
		readings := pinyin.Pinyin(string(ch), args)
		if len(readings) == 0 || len(readings[0]) == 0 {
			continue // 标点、空格、其它符号跳过
		}
		syllable := readings[0][0]
		fb.WriteString(syllable)
		ib.WriteByte(syllable[0])
	}
	return fb.String(), ib.String()
}

// BackfillNameIndex 给历史条目补拼音；分批执行，避免长事务阻塞写入。
func (d *DB) BackfillNameIndex(ctx context.Context) (int, error) {
	updated := 0
	lastID := ""
	for {
		// 按 id 游标翻页：即使某条目的标题算不出拼音（纯符号），也不会被反复选中导致死循环。
		rows, err := d.sql.QueryContext(ctx,
			`SELECT id, name FROM items WHERE coalesce(pinyin,'')='' AND id > ? ORDER BY id LIMIT 500`, lastID)
		if err != nil {
			return updated, err
		}
		type row struct{ id, name string }
		var batch []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.name); err != nil {
				rows.Close()
				return updated, err
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return updated, err
		}
		if len(batch) == 0 {
			return updated, nil
		}
		lastID = batch[len(batch)-1].id
		tx, err := d.sql.BeginTx(ctx, nil)
		if err != nil {
			return updated, err
		}
		stmt, err := tx.PrepareContext(ctx, `UPDATE items SET pinyin=?, pinyin_initials=? WHERE id=?`)
		if err != nil {
			tx.Rollback()
			return updated, err
		}
		for _, r := range batch {
			f, i := nameIndexes(r.name)
			if _, err := stmt.ExecContext(ctx, f, i, r.id); err != nil {
				stmt.Close()
				tx.Rollback()
				return updated, err
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return updated, err
		}
		updated += len(batch)
	}
}

// FillMissingNameIndex 给拼音字段为空的条目补索引。
// 扫描入库时已经同步写入；这里兜底历史/异常数据，保证扫描结束后即可搜索。
func (d *DB) FillMissingNameIndex(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		batch = 1000
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, name FROM items WHERE coalesce(pinyin,'')='' LIMIT ?`, batch)
	if err != nil {
		return 0, err
	}
	type row struct{ id, name string }
	var list []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(list) == 0 {
		return 0, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, r := range list {
		full, initials := nameIndexes(r.name)
		if _, err := tx.ExecContext(ctx,
			`UPDATE items SET pinyin=?, pinyin_initials=? WHERE id=?`, full, initials, r.id); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	markSearchDirty()
	return len(list), nil
}
