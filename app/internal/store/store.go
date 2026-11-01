// Package store 封装 SQLite：连接、迁移与查询。
// 约定：WAL 单写多读；所有方法带 context；禁止在 SQL 中拼接用户输入。
package store

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"emby-air/internal/model"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// PathRecord 是扫描器交给索引层的最小单位。
type PathRecord struct {
	Path   string
	ItemId string
	MTime  int64
	Size   int64
}

// Stats 用于 /System/Info 与健康检查。
type Stats struct {
	Items         int
	Series        int
	Episodes      int
	Movies        int
	Users         int
	LibraryDBSize int64
}

// tokenSeenInterval 是 tokens.last_seen 的写库节流窗口：同一令牌在这个窗口内
// 只写一次库。last_seen 只用于“最近活跃”展示，不值得占用写锁。
const tokenSeenInterval = 5 * time.Minute

// tokenSeenCap 是 last_seen 内存节流表的容量上限；超限整体清空，避免无界增长。
const tokenSeenCap = 4096

// tokenSeenTimeout 是写 last_seen 的独立超时：扫描/维护持写锁时最多等这么久，
// 超时直接放弃（绝不让非关键数据拖慢鉴权）。
const tokenSeenTimeout = 2 * time.Second

// tokenIdleTTL 是令牌空闲过期时间：90 天没有请求的令牌在下一次校验时失效。
// last_seen=0 的历史令牌不立即失效（下一次请求会刷新）。
const tokenIdleTTL = 90 * 24 * time.Hour

// DB 是存储句柄。
type DB struct {
	sql  *sql.DB
	path string
	si   searchIndex

	// seen 记录每个令牌上次写 last_seen 的时间（unix 秒），受 seenMu 保护。
	seenMu sync.Mutex
	seen   map[string]int64
}

// Open 打开（必要时创建）数据库并设置 PRAGMA。
func Open(path string) (*DB, error) {
	dsn := "file:" + path +
		"?_pragma=busy_timeout(15000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// 单写多读：写连接串行化，读连接并发。
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{sql: db, path: path, seen: make(map[string]int64, 64)}, nil
}

// Close 关闭数据库。
func (d *DB) Close() error { return d.sql.Close() }

// schemaVersion 是当前 schema 版本；每次结构变更都必须 +1 并在 migrateV2 之后追加迁移。
const schemaVersion = 16

// v2Columns 是 items 表在 v2 新增的列。ALTER TABLE 不支持 IF NOT EXISTS，
// 因此按 PRAGMA table_info 的结果差量补列；重复执行安全。
var v2Columns = [][2]string{
	{"original_title", "TEXT"},
	{"premiere_date", "TEXT"},
	{"taglines", "TEXT"},
	{"tags", "TEXT"},
	{"studios", "TEXT"},
	{"people", "TEXT"},
	{"image_ref", "TEXT"},
	{"logo_ref", "TEXT"},
	{"locked_fields", "TEXT DEFAULT '[]'"},
	{"backdrop_ref", "TEXT"},
	{"forced_sort_name", "TEXT"},
	{"sort_index", "INTEGER DEFAULT 0"},
	{"scraped_at", "INTEGER DEFAULT 0"},
}

// userColumns 是 users 表新增的策略列（v3）。
var userColumns = [][2]string{
	{"is_disabled", "INTEGER DEFAULT 0"},
	{"max_active_sessions", "INTEGER DEFAULT 0"},
	{"enable_all_folders", "INTEGER DEFAULT 1"},
	{"enabled_folders", "TEXT DEFAULT '[]'"},
	{"blocked_tags", "TEXT DEFAULT '[]'"},
	{"avatar_ref", "TEXT DEFAULT ''"},
}

// Migrate 应用 schema（幂等）。
func (d *DB) Migrate(ctx context.Context) error {
	// 已是最新版本就直接返回：避免 CLI 与服务并发时抢写锁（之前会报 database is locked）。
	var current string
	if err := d.sql.QueryRowContext(ctx, `SELECT v FROM settings WHERE k='schema_version'`).Scan(&current); err == nil &&
		current == strconv.Itoa(schemaVersion) {
		return nil
	}

	// 扫描/监听进程可能正持有写锁：遇到 SQLITE_BUSY 重试几次再放弃。
	var err error
	for i := 0; i < 5; i++ {
		if _, err = d.sql.ExecContext(ctx, schemaSQL); err == nil {
			break
		}
		if !strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy") {
			return err
		}
		time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
	}
	if err != nil {
		return err
	}
	if err := d.migrateV2(ctx); err != nil {
		return err
	}
	_, err = d.sql.ExecContext(ctx,
		`INSERT INTO settings(k,v) VALUES('schema_version',?)
		 ON CONFLICT(k) DO UPDATE SET v=excluded.v`, strconv.Itoa(schemaVersion))
	return err
}

// migrateV2 补齐 v2 列。老库（v1）在这里按 table_info 差量 ALTER；新库同样走一遍（无操作）。
func (d *DB) migrateV2(ctx context.Context) error {
	existing := map[string]bool{}
	rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('items')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range v2Columns {
		if existing[c[0]] {
			continue
		}
		if _, err := d.sql.ExecContext(ctx, "ALTER TABLE items ADD COLUMN "+c[0]+" "+c[1]); err != nil {
			return fmt.Errorf("migrate v2: add %s: %w", c[0], err)
		}
	}
	if err := d.migrateUsers(ctx); err != nil {
		return err
	}
	if err := d.migrateV4(ctx); err != nil {
		return err
	}
	if err := d.migrateV5(ctx); err != nil {
		return err
	}
	if err := d.migrateV6(ctx); err != nil {
		return err
	}
	if err := d.migrateV7(ctx); err != nil {
		return err
	}
	if err := d.migrateV8(ctx); err != nil {
		return err
	}
	if err := d.migrateV9(ctx); err != nil {
		return err
	}
	if err := d.migrateV10(ctx); err != nil {
		return err
	}
	if err := d.migrateV11(ctx); err != nil {
		return err
	}
	if err := d.migrateV12(ctx); err != nil {
		return err
	}
	if err := d.migrateV13(ctx); err != nil {
		return err
	}
	if err := d.migrateV14(ctx); err != nil {
		return err
	}
	if err := d.migrateV15(ctx); err != nil {
		return err
	}
	if err := d.migrateV16(ctx); err != nil {
		return err
	}
	return d.migrateViews(ctx)
}

// v4Columns 是播放会话/进度/令牌在 v4 新增的列。
var v4Columns = map[string][][2]string{
	"user_data": {
		{"duration_ticks", "INTEGER DEFAULT 0"},
		{"updated", "INTEGER DEFAULT 0"},
	},
	"sessions": {
		{"user_name", "TEXT DEFAULT ''"},
		{"item_name", "TEXT DEFAULT ''"},
		{"runtime_ticks", "INTEGER DEFAULT 0"},
		{"started", "INTEGER DEFAULT 0"},
		{"client_version", "TEXT DEFAULT ''"},
		{"play_method", "TEXT DEFAULT ''"},
		{"ip", "TEXT DEFAULT ''"},
	},
	"tokens": {
		{"kind", "TEXT NOT NULL DEFAULT 'client'"},
		{"client_version", "TEXT DEFAULT ''"},
		{"device_name", "TEXT DEFAULT ''"},
	},
}

// v6Columns 是分集缩略图列。
var v6Columns = [][2]string{
	{"tmdb_still", "TEXT DEFAULT ''"},
}

// migrateV6 给 items 补分集缩略图列，并加 path 前缀索引（媒体库统计/路径过滤用）。
func (d *DB) migrateV6(ctx context.Context) error {
	existing := map[string]bool{}
	rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('items')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range v6Columns {
		if existing[c[0]] {
			continue
		}
		if _, err := d.sql.ExecContext(ctx, "ALTER TABLE items ADD COLUMN "+c[0]+" "+c[1]); err != nil {
			return fmt.Errorf("migrate v6: add %s: %w", c[0], err)
		}
	}
	if _, err := d.sql.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_items_path ON items(path)`); err != nil {
		return err
	}
	// 媒体库统计的主查询：kind + path 前缀，联合索引可直接落到范围扫描。
	if _, err := d.sql.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_items_kind_path ON items(kind, path)`); err != nil {
		return err
	}
	// 单集计数/查找：series_id + kind 覆盖索引，避免回表读 162MB 大行。
	_, err = d.sql.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_items_series_kind ON items(series_id, kind)`)
	return err
}

// migrateV15 给 Pansave 增量轮询用的 scraped_at 建索引。
// 查询条件保持 (date_created >= ? OR scraped_at >= ?)，两边各有索引后
// SQLite 可以做多索引 OR，避免每次通知都扫 15 万行。
func (d *DB) migrateV15(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_items_scraped ON items(scraped_at)`)
	return err
}

// migrateV14 首页“最新”排序索引：避免每次 Latest 都对上万行做临时排序。
func (d *DB) migrateV14(ctx context.Context) error {
	for _, sql := range []string{
		`CREATE INDEX IF NOT EXISTS idx_items_kind_premiere ON items(kind, premiere_date DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_items_parent_premiere ON items(parent_id, kind, premiere_date DESC, id DESC)`,
	} {
		if _, err := d.sql.ExecContext(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}

// migrateV13 覆盖索引：媒体库列表先按 (parent, kind, sort_name) 取一页 ID，
// 再回表取 10 行的宽字段，避免为排序读取上万行大记录（HDD 上要 1~2 秒）。
func (d *DB) migrateV13(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_items_parent_kind_sort ON items(parent_id, kind, sort_name, id)`)
	return err
}

// migrateV12 给 parent_id 建索引：媒体库统计与 Emby Items?ParentId 查询
// 都按 parent_id 精确过滤，之前要扫 7 万行。
func (d *DB) migrateV12(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_items_parent_kind ON items(parent_id, kind)`)
	return err
}

// migrateV11 缓存 strm 直链，减少对机械盘的重复读取。
func (d *DB) migrateV11(ctx context.Context) error {
	var has int
	_ = d.sql.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('media_sources') WHERE name='resolved_url'`).Scan(&has)
	if has > 0 {
		return nil
	}
	_, err := d.sql.ExecContext(ctx, `ALTER TABLE media_sources ADD COLUMN resolved_url TEXT DEFAULT ''`)
	return err
}

// migrateV10 给播放源补 size（strm 源文件大小，按需探测后缓存）。
func (d *DB) migrateV10(ctx context.Context) error {
	var has int
	_ = d.sql.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('media_sources') WHERE name='size'`).Scan(&has)
	if has > 0 {
		return nil
	}
	_, err := d.sql.ExecContext(ctx, `ALTER TABLE media_sources ADD COLUMN size INTEGER DEFAULT 0`)
	return err
}

// migrateV9 给剧名查找加索引（孤儿修复按 name 关联，没索引时 2 万行要跑几分钟）。
func (d *DB) migrateV9(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_items_kind_name ON items(kind, name)`)
	return err
}

// migrateV8 给“同剧同集/同季”去重加部分索引，避免分组查询扫全表（实测 29 秒）。
func (d *DB) migrateV8(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_items_ep_group ON items(series_id, season_number, episode_number) WHERE kind='Episode'`,
		`CREATE INDEX IF NOT EXISTS idx_items_season_group ON items(parent_id, season_number) WHERE kind='Season'`,
	} {
		if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// migrateV7 补齐统计用索引。
// 教训：加索引也要升 schemaVersion，否则老库（版本号相同）会直接跳过迁移。
func (d *DB) migrateV7(ctx context.Context) error {
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS idx_items_kind_path ON items(kind, path)`,
		`CREATE INDEX IF NOT EXISTS idx_items_series_kind ON items(series_id, kind)`,
	} {
		if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// v5Columns 是拼音搜索需要的索引列。
var v5Columns = [][2]string{
	{"pinyin", "TEXT DEFAULT ''"},
	{"pinyin_initials", "TEXT DEFAULT ''"},
}

// migrateV5 给 items 补拼音列（老库增量升级）。
func (d *DB) migrateV5(ctx context.Context) error {
	existing := map[string]bool{}
	rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('items')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range v5Columns {
		if existing[c[0]] {
			continue
		}
		if _, err := d.sql.ExecContext(ctx, "ALTER TABLE items ADD COLUMN "+c[0]+" "+c[1]); err != nil {
			return fmt.Errorf("migrate v5: add %s: %w", c[0], err)
		}
	}
	return nil
}

// migrateV4 补齐播放记录/实时会话/设备识别相关列。
// 新表（activities/devices）由 schema.sql 的 CREATE TABLE IF NOT EXISTS 负责，
// 这里只处理 ALTER TABLE 不支持 IF NOT EXISTS 的加列场景。
func (d *DB) migrateV4(ctx context.Context) error {
	for table, cols := range v4Columns {
		existing := map[string]bool{}
		rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('`+table+`')`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				return err
			}
			existing[n] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range cols {
			if existing[c[0]] {
				continue
			}
			if _, err := d.sql.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+c[0]+" "+c[1]); err != nil {
				return fmt.Errorf("migrate v4 %s: add %s: %w", table, c[0], err)
			}
		}
	}
	return nil
}

// migrateUsers 给 users 表补策略列（v3）。
func (d *DB) migrateUsers(ctx context.Context) error {
	existing := map[string]bool{}
	rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('users')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range userColumns {
		if existing[c[0]] {
			continue
		}
		if _, err := d.sql.ExecContext(ctx, "ALTER TABLE users ADD COLUMN "+c[0]+" "+c[1]); err != nil {
			return fmt.Errorf("migrate users: add %s: %w", c[0], err)
		}
	}
	return nil
}

// Ping 用于健康检查。
func (d *DB) Ping(ctx context.Context) error { return d.sql.PingContext(ctx) }

// ---------- 查询 ----------

type where struct {
	sql  []string
	args []any
}

func (w *where) add(cond string, args ...any) {
	w.sql = append(w.sql, cond)
	w.args = append(w.args, args...)
}

func (w *where) String() string {
	if len(w.sql) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.sql, " AND ")
}

// QueryItems 按查询参数分页返回条目。
func (d *DB) QueryItems(ctx context.Context, q model.ItemsQuery) (model.Page, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 60
	}
	if limit > 500 {
		limit = 500
	}

	var w where
	if q.ParentId != "" {
		if q.Recursive {
			// Emby 的 Recursive=true 要返回整棵子树（Series→Season→Episode），
			// 不能只按 parent_id 取直接子级。
			w.add("id IN (WITH RECURSIVE tree(id) AS ("+
				"SELECT id FROM items WHERE parent_id = ? "+
				"UNION SELECT i.id FROM items i JOIN tree t ON i.parent_id = t.id"+
				") SELECT id FROM tree)", q.ParentId)
		} else {
			w.add("parent_id = ?", q.ParentId)
		}
	} else if len(q.AllowedParents) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(q.AllowedParents)), ",")
		args := make([]any, len(q.AllowedParents))
		for i, id := range q.AllowedParents {
			args[i] = id
		}
		if q.Recursive {
			// 全局递归 + 库权限：只展开授权媒体库的整棵子树。
			w.add("id IN (WITH RECURSIVE tree(id) AS ("+
				"SELECT id FROM items WHERE parent_id IN ("+ph+") "+
				"UNION SELECT i.id FROM items i JOIN tree t ON i.parent_id = t.id"+
				") SELECT id FROM tree)", args...)
		} else {
			w.add("parent_id IN ("+ph+")", args...)
		}
	}
	if len(q.Types) > 0 {
		ph := make([]string, len(q.Types))
		for i, t := range q.Types {
			ph[i] = "?"
			w.args = append(w.args, t)
		}
		w.sql = append(w.sql, "kind IN ("+strings.Join(ph, ",")+")")
	}
	if len(q.ExcludeTypes) > 0 {
		// Emby 的 ExcludeItemTypes：避免爆米花等客户端在库内列表里混入 Season/Episode。
		ph := strings.TrimSuffix(strings.Repeat("?,", len(q.ExcludeTypes)), ",")
		args := make([]any, len(q.ExcludeTypes))
		for i, t := range q.ExcludeTypes {
			args[i] = t
		}
		w.add("kind NOT IN ("+ph+")", args...)
	}
	if q.SearchTerm != "" {
		// 内存索引匹配（中文/全拼/首字母），避免 75k 行上的 LIKE 全表扫描。
		ids, ok := d.SearchIDs(context.Background(), q.SearchTerm, 2000)
		if ok && len(ids) == 0 {
			return model.Page{Items: []model.Item{}, TotalRecordCount: 0, StartIndex: q.StartIndex}, nil
		}
		if ok {
			w.sql = append(w.sql, "id IN ("+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+")")
			for _, id := range ids {
				w.args = append(w.args, id)
			}
		}
	}
	if q.AnyProviderId != "" {
		// 形式：tmdb.12345 / imdb.tt123 / tmdb:12345
		sep := "."
		if !strings.Contains(q.AnyProviderId, ".") && strings.Contains(q.AnyProviderId, ":") {
			sep = ":"
		}
		parts := strings.SplitN(q.AnyProviderId, sep, 2)
		if len(parts) == 2 {
			w.add("provider_ids LIKE ?", "%\""+strings.ToLower(parts[0])+"\":\""+parts[1]+"\"%")
		}
	}
	if q.MinDateLastSaved > 0 {
		w.add("(date_created >= ? OR scraped_at >= ?)", q.MinDateLastSaved, q.MinDateLastSaved)
	}
	if len(q.Ids) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(q.Ids)), ",")
		args := make([]any, len(q.Ids))
		for i, id := range q.Ids {
			args[i] = id
		}
		w.add("id IN ("+ph+")", args...)
	}
	if len(q.Years) > 0 {
		ph := make([]string, len(q.Years))
		for i, y := range q.Years {
			ph[i] = "?"
			w.args = append(w.args, y)
		}
		w.sql = append(w.sql, "year IN ("+strings.Join(ph, ",")+")")
	}
	for _, g := range q.Genres {
		w.add("genres LIKE ?", "%\""+g+"\"%")
	}
	for _, f := range q.Filters {
		switch strings.ToLower(f) {
		case "isplayed":
			w.add("id IN (SELECT item_id FROM user_data WHERE user_id=? AND played=1)", q.UserId)
		case "isunplayed":
			w.add("id NOT IN (SELECT item_id FROM user_data WHERE user_id=? AND played=1)", q.UserId)
		case "isfavorite":
			w.add("id IN (SELECT item_id FROM user_data WHERE user_id=? AND is_favorite=1)", q.UserId)
		}
	}
	if q.IsPlayed != nil {
		if *q.IsPlayed {
			w.add("id IN (SELECT item_id FROM user_data WHERE user_id=? AND played=1)", q.UserId)
		} else {
			w.add("id NOT IN (SELECT item_id FROM user_data WHERE user_id=? AND played=1)", q.UserId)
		}
	}
	if q.IsFavorite != nil {
		if *q.IsFavorite {
			w.add("id IN (SELECT item_id FROM user_data WHERE user_id=? AND is_favorite=1)", q.UserId)
		} else {
			w.add("id NOT IN (SELECT item_id FROM user_data WHERE user_id=? AND is_favorite=1)", q.UserId)
		}
	}

	order := orderClause(q.SortBy, q.SortOrder)
	offset := q.StartIndex
	if offset < 0 {
		offset = 0
	}

	rowsSQL := `SELECT ` + itemColumns + ` FROM items` + w.String() +
		" ORDER BY " + order + " LIMIT ? OFFSET ?"
	args := append(append([]any{}, w.args...), limit, offset)

	rows, err := d.sql.QueryContext(ctx, rowsSQL, args...)
	if err != nil {
		return model.Page{}, err
	}
	defer rows.Close()

	items := make([]model.Item, 0, limit)
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return model.Page{}, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return model.Page{}, err
	}

	var total int
	countSQL := "SELECT count(*) FROM items" + w.String()
	if err := d.sql.QueryRowContext(ctx, countSQL, w.args...).Scan(&total); err != nil {
		return model.Page{}, err
	}

	if len(items) > 0 && q.UserId != "" {
		if err := d.attachUserData(ctx, q.UserId, items); err != nil {
			return model.Page{}, err
		}
	}
	return model.Page{Items: items, TotalRecordCount: total, StartIndex: offset}, nil
}

func isDateSort(sortBy []string) bool {
	for _, s := range sortBy {
		switch strings.ToLower(s) {
		case "datecreated", "premieredate", "dateplayed", "dateadded":
			return true
		}
	}
	return false
}

func orderClause(sortBy []string, sortOrder string) string {
	dir := "ASC"
	if strings.EqualFold(sortOrder, "Descending") {
		dir = "DESC"
	}
	// 客户端（VidHub 首页）经常只传 SortBy=DateCreated、不传 SortOrder。
	// 日期类排序的默认方向必须是“新→旧”，否则最新发行反而排最后。
	if strings.TrimSpace(sortOrder) == "" && isDateSort(sortBy) {
		dir = "DESC"
	}
	var parts []string
	for _, s := range sortBy {
		switch strings.ToLower(s) {
		case "random":
			parts = append(parts, "random()")
		case "sortname", "name":
			parts = append(parts, "sort_name "+dir)
		case "datecreated":
			// 按发行日排序（用户要求：主页不要按“加入时间”）。
			// premiere_date 缺失时回落到年份，保证老片也有稳定顺序。
			parts = append(parts, "COALESCE(NULLIF(premiere_date,''), printf('%04d-01-01', year), '0000') "+dir)
		case "premieredate", "dateadded":
			parts = append(parts, "COALESCE(NULLIF(premiere_date,''), printf('%04d-01-01', year), '0000') "+dir)
		case "communityrating":
			parts = append(parts, "community_rating "+dir)
		case "runtime":
			parts = append(parts, "runtime_ticks "+dir)
		case "parentindexnumber":
			parts = append(parts, "season_number "+dir)
		case "indexnumber":
			parts = append(parts, "episode_number "+dir)
		}
	}
	if len(parts) == 0 {
		parts = append(parts, "sort_name ASC")
	}
	// id 作为稳定 tiebreaker，保证分页不重复不丢
	parts = append(parts, "id "+dir)
	return strings.Join(parts, ", ")
}

type scanner interface {
	Scan(dest ...any) error
}

// itemColumns 是 items 表的投影列清单；scanItem 的 Scan 顺序必须与之严格一致。
// 新增列时两处一起改（有 TestItemColumnsMatchScanItem 兜底）。
const itemColumns = "id, kind, name, sort_name, parent_id, series_id, series_name, season_id, " +
	"season_number, episode_number, year, runtime_ticks, overview, official_rating, " +
	"community_rating, path, container, genres, image_tags, backdrop_tags, " +
	"provider_ids, date_created, sort_index, original_title, premiere_date, taglines, " +
	"tags, studios, people, image_ref, backdrop_ref, logo_ref, forced_sort_name, scraped_at, tmdb_still"

func scanItem(s scanner) (model.Item, error) {
	var it model.Item
	var kind, genres, imageTags, backdropTags, providerIds sql.NullString
	var parentId, seriesId, seriesName, seasonId, overview, rating, path, container sql.NullString
	var originalTitle, premiereDate, taglines, tags, studios, people sql.NullString
	var imageRef, backdropRef, logoRef, forcedSort, tmdbStill sql.NullString
	var seasonNum, epNum, year, sortIndex, scrapedAt sql.NullInt64
	var runtimeTicks sql.NullInt64
	var community float64
	var created int64

	if err := s.Scan(&it.Id, &kind, &it.Name, &it.SortName, &parentId, &seriesId, &seriesName, &seasonId,
		&seasonNum, &epNum, &year, &runtimeTicks, &overview, &rating, &community,
		&path, &container, &genres, &imageTags, &backdropTags, &providerIds,
		&created, &sortIndex, &originalTitle, &premiereDate, &taglines, &tags,
		&studios, &people, &imageRef, &backdropRef, &logoRef, &forcedSort, &scrapedAt, &tmdbStill); err != nil {
		return it, err
	}

	it.Type = kind.String
	it.ParentId = parentId.String
	it.SeriesId = seriesId.String
	it.SeriesName = seriesName.String
	it.SeasonId = seasonId.String
	it.Overview = overview.String
	it.OfficialRating = rating.String
	it.CommunityRating = community
	it.Path = path.String
	it.Container = container.String
	it.ProductionYear = int(year.Int64)
	it.IndexNumber = int(epNum.Int64)
	it.ParentIndexNumber = int(seasonNum.Int64)
	it.RunTimeTicks = runtimeTicks.Int64
	it.IsFolder = strings.EqualFold(it.Type, "Series") || strings.EqualFold(it.Type, "Season") ||
		strings.EqualFold(it.Type, "Folder")
	it.DateCreated = time.Unix(created, 0).UTC().Format(time.RFC3339)

	if genres.Valid && genres.String != "" {
		_ = json.Unmarshal([]byte(genres.String), &it.Genres)
	}
	if imageTags.Valid && imageTags.String != "" {
		_ = json.Unmarshal([]byte(imageTags.String), &it.ImageTags)
	}
	if backdropTags.Valid && backdropTags.String != "" {
		_ = json.Unmarshal([]byte(backdropTags.String), &it.BackdropImageTags)
	}
	if providerIds.Valid && providerIds.String != "" {
		_ = json.Unmarshal([]byte(providerIds.String), &it.ProviderIds)
	}

	it.OriginalTitle = originalTitle.String
	it.PremiereDate = premiereDate.String
	it.ForcedSortName = forcedSort.String
	it.ImageRef = imageRef.String
	it.BackdropRef = backdropRef.String
	it.LogoRef = logoRef.String

	// 客户端只认 ImageTags；本地/缓存图存在 ImageRef 里，这里合成一个稳定 tag，
	// 否则客户端不会发图片请求（海报墙全空）。
	// 分集缩略图是懒加载的：有 TMDB still 就先给个 tag，客户端才会去请求，
	// 请求时再由 handleImage 按需下载并缓存。
	if len(it.ImageTags) == 0 && tmdbStill.Valid && tmdbStill.String != "" {
		it.ImageTags = map[string]string{"Primary": "still"}
	}
	if len(it.ImageTags) == 0 && it.ImageRef != "" {
		it.ImageTags = map[string]string{"Primary": imageVersionTag(it.ImageRef)}
	}
	// 背景图同理：客户端只认 BackdropImageTags。
	if len(it.BackdropImageTags) == 0 && it.BackdropRef != "" {
		it.BackdropImageTags = []string{imageVersionTag(it.BackdropRef)}
	}
	// Logo 也补一个 tag，部分客户端会单独请求。
	// 后缀 -png 用于让客户端重新拉取原图：旧版本曾把透明 PNG 转成 JPEG 缩略图，
	// 客户端按 tag 缓存后会一直显示黑底；tag 变化才会重新请求。
	if it.LogoRef != "" {
		if it.ImageTags == nil {
			it.ImageTags = map[string]string{}
		}
		it.ImageTags["Logo"] = imageVersionTag(it.LogoRef) + "-png"
	}
	if taglines.Valid && taglines.String != "" {
		_ = json.Unmarshal([]byte(taglines.String), &it.Taglines)
	}
	if tags.Valid && tags.String != "" {
		_ = json.Unmarshal([]byte(tags.String), &it.Tags)
	}
	if studios.Valid && studios.String != "" {
		_ = json.Unmarshal([]byte(studios.String), &it.Studios)
	}
	if people.Valid && people.String != "" {
		_ = json.Unmarshal([]byte(people.String), &it.People)
	}
	_ = scrapedAt // 仅用于增量判定，不下发
	return it, nil
}

// AttachUserDataForList 给一批条目补 UserData（列表/剧集接口使用）。
func (d *DB) AttachUserDataForList(ctx context.Context, userId string, items []model.Item) error {
	if userId == "" || len(items) == 0 {
		return nil
	}
	return d.attachUserData(ctx, userId, items)
}

// NextUpEpisodes 返回“下一集”：优先正在看但没看完的那一集；都没进度时取最早未观看的一集。
func (d *DB) NextUpEpisodes(ctx context.Context, userID, seriesID string, limit int) ([]model.Item, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	seasonFilter, seasonArg := "", []any{}
	if seriesID != "" {
		seasonFilter = " AND i.series_id=?"
		seasonArg = append(seasonArg, seriesID)
	}

	q := `SELECT ` + itemColumns + ` FROM items i
		JOIN user_data ud ON ud.item_id=i.id AND ud.user_id=?
		WHERE i.kind='Episode'` + seasonFilter + `
		  AND ud.played=0 AND ud.position_ticks>0
		  AND (ud.duration_ticks=0 OR ud.position_ticks < ud.duration_ticks*9/10)
		ORDER BY ud.updated DESC LIMIT ?`
	args := append([]any{userID}, seasonArg...)
	args = append(args, limit)
	items, err := d.queryItemsList(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		return items, nil
	}

	q2 := `SELECT ` + itemColumns + ` FROM items i WHERE i.kind='Episode'` + seasonFilter + `
		AND NOT EXISTS (SELECT 1 FROM user_data ud WHERE ud.item_id=i.id AND ud.user_id=?)
		ORDER BY i.season_number, i.episode_number LIMIT ?`
	args2 := append(append([]any{}, seasonArg...), userID, limit)
	return d.queryItemsList(ctx, q2, args2...)
}

// AttachUserData 给单个条目补 UserData（详情接口必须带，客户端据此决定续播位置）。
func (d *DB) AttachUserData(ctx context.Context, userId string, it *model.Item) error {
	if userId == "" || it == nil {
		return nil
	}
	items := []model.Item{*it}
	if err := d.attachUserData(ctx, userId, items); err != nil {
		return err
	}
	*it = items[0]
	return nil
}

func (d *DB) attachUserData(ctx context.Context, userId string, items []model.Item) error {
	ids := make([]string, len(items))
	for i := range items {
		ids[i] = items[i].Id
	}
	// 按批查询，避免 IN 过长
	const chunk = 200
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		ph := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		q := `SELECT item_id, played, play_count, position_ticks, is_favorite, last_played
			FROM user_data WHERE user_id=? AND item_id IN (` + ph + `)`
		args := []any{userId}
		for _, id := range ids[start:end] {
			args = append(args, id)
		}
		rows, err := d.sql.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var itemId string
			var played, playCount, fav int
			var pos, last sql.NullInt64
			if err := rows.Scan(&itemId, &played, &playCount, &pos, &fav, &last); err != nil {
				rows.Close()
				return err
			}
			for i := range items {
				if items[i].Id != itemId {
					continue
				}
				ud := &model.UserData{
					ItemId:                itemId,
					Played:                played == 1,
					PlayCount:             playCount,
					PlaybackPositionTicks: pos.Int64,
					IsFavorite:            fav == 1,
					Key:                   fmt.Sprintf("%s-%s", userId, itemId),
				}
				if items[i].RunTimeTicks > 0 {
					ud.PlayedPercentage = float64(pos.Int64) / float64(items[i].RunTimeTicks) * 100
				}
				if last.Valid {
					ud.LastPlayedDate = time.Unix(last.Int64, 0).UTC().Format(time.RFC3339)
				}
				items[i].UserData = ud
			}
		}
		rows.Close()
	}
	return nil
}

// GetItem 返回单个条目（含 MediaSources）。
// ItemExists 只判断条目是否存在，用于图片缓存孤儿清理。
func (d *DB) ItemExists(ctx context.Context, id string) (bool, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT 1 FROM items WHERE id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ViewExists 判断媒体库是否存在。
func (d *DB) ViewExists(ctx context.Context, id string) (bool, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT 1 FROM views WHERE id=?`, id).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ItemImageInfo 是图片端点需要的轻量字段，避免每张封面都完整加载 item + media_sources。
type ItemImageInfo struct {
	Path        string
	Type        string
	ImageRef    string
	BackdropRef string
	LogoRef     string
}

func (d *DB) ItemImageInfo(ctx context.Context, id string) (*ItemImageInfo, error) {
	var v ItemImageInfo
	err := d.sql.QueryRowContext(ctx,
		`SELECT coalesce(path,''), kind, coalesce(image_ref,''), coalesce(backdrop_ref,''), coalesce(logo_ref,'')
		 FROM items WHERE id=?`, id).Scan(&v.Path, &v.Type, &v.ImageRef, &v.BackdropRef, &v.LogoRef)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (d *DB) GetItem(ctx context.Context, id string) (*model.Item, error) {
	row := d.sql.QueryRowContext(ctx, `SELECT `+itemColumns+` FROM items WHERE id=?`, id)
	it, err := scanItem(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if it.Type == "Movie" || it.Type == "Episode" {
		rows, err := d.sql.QueryContext(ctx,
			`SELECT source_id, name, path, container, runtime_ticks, coalesce(size,0), coalesce(resolved_url,'')
			 FROM media_sources WHERE item_id=? ORDER BY sort_order, source_id`, id)
		if err == nil {
			for rows.Next() {
				var sid, name, p, container, resolved string
				var rt, size int64
				if err := rows.Scan(&sid, &name, &p, &container, &rt, &size, &resolved); err != nil {
					break
				}
				ms := newMediaSource(it.Name, sid, name, p, container, resolved, rt, size)
				if resolved == "" && ms.Path != "" {
					// 首次解析：顺手回填缓存，之后不再读盘（机械盘少唤醒一次）。
					_, _ = d.sql.ExecContext(ctx, `UPDATE media_sources SET resolved_url=? WHERE item_id=? AND source_id=?`, ms.Path, it.Id, sid)
				}
				it.MediaSources = append(it.MediaSources, ms)
			}
			rows.Close()
		}
		if len(it.MediaSources) > 1 {
			seen := map[string]bool{}
			uniq := it.MediaSources[:0]
			for _, ms := range it.MediaSources {
				if seen[ms.Path] {
					continue
				}
				seen[ms.Path] = true
				uniq = append(uniq, ms)
			}
			it.MediaSources = uniq
		}
		if len(it.MediaSources) == 0 && it.Path != "" {
			it.MediaSources = []model.MediaSource{newMediaSource(it.Name, it.Id, "", it.Path, it.Container, "", it.RunTimeTicks, 0)}
		}
	}
	return &it, nil
}

// AttachMediaSources 为列表结果批量补 MediaSources（Emby 列表默认不带，
// 但客户端（如 Filmly）会用列表里的 MediaSources 判断条目是否可播放，
// 缺了它就会认为影片“进不去详情页”。
//
// 与 GetItem 的区别：只读取已缓存的 resolved_url，绝不回写
// （列表动辄上百条，回写会把机械盘/DB 打满）。
func (d *DB) AttachMediaSources(ctx context.Context, items []model.Item) error {
	if len(items) == 0 {
		return nil
	}
	pos := make(map[string][]int, len(items))
	ids := make([]string, 0, len(items))
	for i := range items {
		it := &items[i]
		if it.Type != "Movie" && it.Type != "Episode" {
			continue
		}
		if _, ok := pos[it.Id]; !ok {
			ids = append(ids, it.Id)
		}
		pos[it.Id] = append(pos[it.Id], i)
	}
	if len(ids) == 0 {
		return nil
	}
	found := make(map[string][]model.MediaSource, len(ids))
	const chunk = 400 // 低于 SQLite 默认变量上限（999），留足余量
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		part := ids[start:end]
		ph := strings.TrimSuffix(strings.Repeat("?,", len(part)), ",")
		args := make([]any, len(part))
		for i, id := range part {
			args[i] = id
		}
		rows, err := d.sql.QueryContext(ctx,
			`SELECT item_id, source_id, name, path, container, coalesce(runtime_ticks,0), coalesce(size,0), coalesce(resolved_url,'')
			 FROM media_sources WHERE item_id IN (`+ph+`) ORDER BY item_id, sort_order, source_id`, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var itemID, sid, name, p, container, resolved string
			var rt, size int64
			if err := rows.Scan(&itemID, &sid, &name, &p, &container, &rt, &size, &resolved); err != nil {
				rows.Close()
				return err
			}
			label := name
			if label == "" {
				if list := pos[itemID]; len(list) > 0 {
					label = items[list[0]].Name
				}
			}
			found[itemID] = append(found[itemID], newMediaSource(label, sid, name, p, container, resolved, rt, size))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
	}
	for _, id := range ids {
		srcs := dedupeBySourceID(found[id])
		for _, i := range pos[id] {
			if len(srcs) > 0 {
				items[i].MediaSources = srcs
				continue
			}
			// 没有 media_sources 行（历史数据/非扫描入口）：与 GetItem 一致，用条目自身兜底。
			if items[i].Path != "" {
				items[i].MediaSources = []model.MediaSource{newMediaSource(items[i].Name, items[i].Id, "", items[i].Path, items[i].Container, "", items[i].RunTimeTicks, 0)}
			}
		}
	}
	return nil
}

// dedupeBySourceID 按 source_id 去重（保留首次出现），避免同一来源重复下发。
func dedupeBySourceID(srcs []model.MediaSource) []model.MediaSource {
	if len(srcs) < 2 {
		return srcs
	}
	seen := make(map[string]bool, len(srcs))
	out := srcs[:0]
	for _, ms := range srcs {
		if seen[ms.Id] {
			continue
		}
		seen[ms.Id] = true
		out = append(out, ms)
	}
	return out
}

// newMediaSource 是 MediaSource 的唯一构造入口：GetItem 和列表批量附加共用它，
// 保证 Container / Protocol / IsRemote / MediaStreams / DefaultAudioStreamIndex
// 在两处完全一致（否则客户端在不同接口里会看到不同的“同一部片”）。
//
// path 是 DB 里记录的来源路径（通常是 .strm 指针文件），resolved 是缓存的直链。
func newMediaSource(itemName, sourceID, name, path, container, resolved string, rt, size int64) model.MediaSource {
	label := name
	if label == "" {
		label = itemName
	}
	target := cachedStrm(path, resolved)
	src := model.MediaSource{
		Id: sourceID, Name: label, Path: target, LocalPath: path,
		// Container 绝不能下发 "strm"：那是 Emby 里不存在的容器名，
		// 客户端拿它去做解码/封装判断会直接失败。
		Container:    realContainer(container, target, path),
		RunTimeTicks: rt, Size: size,
		SupportsDirectPlay: true, SupportsDirectStream: true, SupportsTranscoding: false,
		MediaStreams: streamsForSource(label, path),
	}
	if isRemotePath(target) {
		src.Protocol, src.IsRemote = "Http", true
	} else {
		src.Protocol, src.IsRemote = "File", false
	}
	if idx := defaultAudioIndex(src.MediaStreams); idx != nil {
		src.DefaultAudioStreamIndex = idx
	}
	return src
}

// isRemotePath 判断播放目标是远端 HTTP(S) 直链还是本地文件。
func isRemotePath(p string) bool {
	lp := strings.ToLower(strings.TrimSpace(p))
	return strings.HasPrefix(lp, "http://") || strings.HasPrefix(lp, "https://")
}

// defaultAudioIndex 返回默认音轨的 Index（没有音轨时返回 nil）。
func defaultAudioIndex(streams []model.MediaStream) *int {
	for i := range streams {
		if streams[i].Type == "Audio" && streams[i].IsDefault {
			idx := streams[i].Index
			return &idx
		}
	}
	for i := range streams {
		if streams[i].Type == "Audio" {
			idx := streams[i].Index
			return &idx
		}
	}
	return nil
}

// knownContainers 是允许作为 MediaSource.Container 下发的扩展名白名单。
var knownContainers = map[string]bool{
	"mkv": true, "mp4": true, "m4v": true, "ts": true, "m2ts": true, "mts": true,
	"avi": true, "mov": true, "flv": true, "webm": true, "wmv": true, "mpg": true,
	"mpeg": true, "iso": true, "rmvb": true, "rm": true, "vob": true, "ogv": true,
	"3gp": true, "m2v": true,
}

// realContainer 推导真实容器：DB 值有效时直接用；为空或 "strm" 时从直链/指针文件名推断。
func realContainer(container, target, path string) string {
	c := strings.ToLower(strings.TrimSpace(container))
	if knownContainers[c] {
		return c
	}
	if ext := containerFromPath(target); ext != "" {
		return ext
	}
	return containerFromPath(path)
}

// containerFromPath 从 URL/路径推断容器扩展名：优先 file_name 查询参数（云盘直链常见），
// 其次路径扩展名。推断不出（或只推断出 "strm"）时返回空串。
func containerFromPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	candidates := []string{raw}
	if u, err := url.Parse(raw); err == nil {
		if fn := u.Query().Get("file_name"); fn != "" {
			candidates = append([]string{fn}, candidates...)
		}
		if u.Path != "" {
			candidates = append(candidates, u.Path)
		}
	}
	for _, c := range candidates {
		if i := strings.IndexAny(c, "?#"); i >= 0 {
			c = c[:i]
		}
		ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(c), "."))
		if knownContainers[ext] {
			return ext
		}
	}
	return ""
}

// cachedStrm 优先用数据库缓存的直链，只有缓存为空时才读 .strm（会唤醒机械盘）。
func cachedStrm(p, cached string) string {
	if cached != "" {
		return cached
	}
	return resolveStrm(p)
}

// CacheSourceURL 记录 strm 内直链，后续读取不再触碰磁盘。
func (d *DB) CacheSourceURL(ctx context.Context, itemID, sourceID, url string) {
	if url == "" {
		return
	}
	_, _ = d.sql.ExecContext(ctx, `UPDATE media_sources SET resolved_url=? WHERE item_id=? AND source_id=?`, url, itemID, sourceID)
}

// resolveStrm 若 path 指向 .strm 指针文件，返回其中的直链（首个非空非注释行）。
func resolveStrm(p string) string {
	if !strings.EqualFold(filepath.Ext(p), ".strm") {
		return p
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v := strings.TrimSpace(line); v != "" && !strings.HasPrefix(v, "#") {
			return v
		}
	}
	return ""
}

// ErrNotFound 表示条目不存在。
var ErrNotFound = errors.New("not found")

// GetViews 返回媒体库列表。
func (d *DB) GetViews(ctx context.Context) ([]model.View, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, name, coalesce(collection_type,''), sort_order, coalesce(paths,'[]') FROM views ORDER BY sort_order, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.View
	for rows.Next() {
		var v model.View
		var paths string
		if err := rows.Scan(&v.Id, &v.Name, &v.CollectionType, &v.SortOrder, &paths); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(paths), &v.Paths)
		v.Type = "CollectionFolder"
		v.IsFolder = true
		out = append(out, v)
	}
	return out, rows.Err()
}

// ---------- 用户与令牌 ----------

func hashPassword(password, salt string) string {
	sum := sha256.Sum256([]byte(salt + ":" + password))
	return salt + "$" + hex.EncodeToString(sum[:])
}

func verifyPassword(stored, password string) bool {
	parts := strings.SplitN(stored, "$", 2)
	if len(parts) != 2 {
		return false
	}
	return hashPassword(password, parts[0]) == stored
}

// UserByName 返回用户与其密码哈希。
func (d *DB) UserByName(ctx context.Context, name string) (*model.User, string, error) {
	var u model.User
	var hash string
	var isAdmin int
	var disabled, maxSessions, allFolders int
	var foldersJSON, tagsJSON sql.NullString
	err := d.sql.QueryRowContext(ctx,
		`SELECT id, name, password_hash, is_admin,
		        coalesce(is_disabled,0), coalesce(max_active_sessions,0),
		        coalesce(enable_all_folders,1), coalesce(enabled_folders,'[]'), coalesce(blocked_tags,'[]')
		 FROM users WHERE lower(name)=lower(?)`, name).
		Scan(&u.Id, &u.Name, &hash, &isAdmin, &disabled, &maxSessions, &allFolders, &foldersJSON, &tagsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	u.HasPassword = hash != ""
	u.IsAdmin = isAdmin == 1
	applyUserPolicy(&u, disabled, maxSessions, allFolders, foldersJSON.String, tagsJSON.String)
	return &u, hash, nil
}

// scanUserPolicy 从一行里读出 policy 相关列（is_disabled, max_active_sessions,
// enable_all_folders, enabled_folders, blocked_tags）。
func applyUserPolicy(u *model.User, disabled, maxSessions, allFolders int, foldersJSON, tagsJSON string) {
	p := &model.UserPolicy{
		IsAdministrator:   u.IsAdmin,
		IsDisabled:        disabled == 1,
		MaxActiveSessions: maxSessions,
		EnableAllFolders:  allFolders == 1,
	}
	if foldersJSON != "" {
		_ = json.Unmarshal([]byte(foldersJSON), &p.EnabledFolders)
	}
	if tagsJSON != "" {
		_ = json.Unmarshal([]byte(tagsJSON), &p.BlockedTags)
	}
	u.Policy = p
}

// VerifyPassword 校验明文密码。
func (d *DB) VerifyPassword(stored, password string) bool { return verifyPassword(stored, password) }

// embyPasswordHash 生成 Emby 原生格式 "<base64(salt)>$<base64(pbkdf2-sha1)>"，
// 与 httpapi 的校验实现对齐（ASP.NET Rfc2898DeriveBytes: SHA1, 1000 轮, 32 字节）。
func embyPasswordHash(password string) string {
	salt := make([]byte, 16)
	_, _ = rand.Read(salt)
	derived := pbkdf2SHA1([]byte(password), salt, 1000, 32)
	return base64.StdEncoding.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(derived)
}

func pbkdf2SHA1(password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(sha1.New, password)
	out := make([]byte, 0, keyLen)
	var counter [4]byte
	for block := 1; len(out) < keyLen; block++ {
		counter[0] = byte(block >> 24)
		counter[1] = byte(block >> 16)
		counter[2] = byte(block >> 8)
		counter[3] = byte(block)
		var u hash.Hash = prf
		u.Reset()
		u.Write(salt)
		u.Write(counter[:])
		sum := u.Sum(nil)
		t := append([]byte(nil), sum...)
		for range iter - 1 {
			u.Reset()
			u.Write(sum)
			sum = u.Sum(nil)
			for k := range t {
				t[k] ^= sum[k]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// CreateUser 创建用户。
func (d *DB) CreateUser(ctx context.Context, name, password string, admin bool) (*model.User, error) {
	if name == "" {
		return nil, errors.New("empty name")
	}
	idSum := sha256.Sum256([]byte("user:" + strings.ToLower(name)))
	id := hex.EncodeToString(idSum[:16])
	hash := embyPasswordHash(password)
	isAdmin := 0
	if admin {
		isAdmin = 1
	}
	_, err := d.sql.ExecContext(ctx,
		`INSERT INTO users(id,name,password_hash,is_admin,created) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET name=excluded.name, password_hash=excluded.password_hash`,
		id, name, hash, isAdmin, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return &model.User{Id: id, Name: name, HasPassword: true, IsAdmin: admin}, nil
}

// CreateToken 持久化登录令牌。
func (d *DB) CreateToken(ctx context.Context, token, userId, deviceId string) error {
	now := time.Now().Unix()
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx,
			`INSERT INTO tokens(token,user_id,device_id,created,last_seen) VALUES(?,?,?,?,?)
			 ON CONFLICT(token) DO UPDATE SET last_seen=excluded.last_seen`,
			token, userId, deviceId, now, now)
		return err
	})
}

// CreateTokenKind 持久化令牌并记录来源类型与客户端信息。
func (d *DB) CreateTokenKind(ctx context.Context, token, userID, deviceID, kind, client, version, deviceName string) error {
	now := time.Now().Unix()
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx, `
			INSERT INTO tokens(token,user_id,device_id,client,created,last_seen,kind,client_version,device_name)
			VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(token) DO UPDATE SET last_seen=excluded.last_seen`,
			token, userID, deviceID, client, now, now, kind, version, deviceName)
		return err
	})
}

// UserWithHash 返回用户与密码哈希，供登录校验使用。
func (d *DB) UserWithHash(ctx context.Context, name string) (*model.User, string, error) {
	var u model.User
	var hash string
	var isAdmin, disabled, maxSessions, allFolders int
	var foldersJSON, tagsJSON sql.NullString
	err := d.sql.QueryRowContext(ctx, `
		SELECT id,name,password_hash,is_admin,
		       coalesce(is_disabled,0), coalesce(max_active_sessions,0),
		       coalesce(enable_all_folders,1), coalesce(enabled_folders,'[]'), coalesce(blocked_tags,'[]')
		FROM users WHERE name=? LIMIT 1`, name).
		Scan(&u.Id, &u.Name, &hash, &isAdmin, &disabled, &maxSessions, &allFolders, &foldersJSON, &tagsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	u.HasPassword = hash != ""
	u.IsAdmin = isAdmin == 1
	applyUserPolicy(&u, disabled, maxSessions, allFolders, foldersJSON.String, tagsJSON.String)
	return &u, hash, nil
}

// DeleteTokenByValue 让单个令牌失效（禁用用户时调用）。
func (d *DB) DeleteTokenByValue(ctx context.Context, token string) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM tokens WHERE token=?`, token)
	return err
}

// DeleteTokensForUser 让某个用户的全部令牌失效（库权限变化后强制客户端重新登录/刷新库列表）。
func (d *DB) DeleteTokensForUser(ctx context.Context, userID string) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM tokens WHERE user_id=?`, userID)
	return err
}

// NewToken 生成 32 字节随机 hex。
func NewToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// UserByToken 用令牌换取用户。
func (d *DB) UserByToken(ctx context.Context, token string) (*model.User, error) {
	var u model.User
	var isAdmin int
	var disabled, maxSessions, allFolders, lastSeen int
	var foldersJSON, tagsJSON sql.NullString
	err := d.sql.QueryRowContext(ctx,
		`SELECT u.id, u.name, u.is_admin,
		        coalesce(u.is_disabled,0), coalesce(u.max_active_sessions,0),
		        coalesce(u.enable_all_folders,1), coalesce(u.enabled_folders,'[]'), coalesce(u.blocked_tags,'[]'),
		        coalesce(t.last_seen,0)
		 FROM tokens t JOIN users u ON u.id=t.user_id WHERE t.token=?`, token).
		Scan(&u.Id, &u.Name, &isAdmin, &disabled, &maxSessions, &allFolders, &foldersJSON, &tagsJSON, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if lastSeen > 0 && time.Since(time.Unix(int64(lastSeen), 0)) > tokenIdleTTL {
		_, _ = d.sql.ExecContext(ctx, `DELETE FROM tokens WHERE token=?`, token)
		return nil, ErrNotFound
	}
	u.HasPassword = true
	u.IsAdmin = isAdmin == 1
	applyUserPolicy(&u, disabled, maxSessions, allFolders, foldersJSON.String, tagsJSON.String)
	// 节流写 last_seen：绝大多数请求不再触发写事务，避免在扫描/维护持写锁时
	// 排队到 busy_timeout（线上表现为只读接口 p95 1.8s、个别 10–60s 后 499）。
	d.markTokenSeen(token)
	return &u, nil
}

// markTokenSeen 在节流窗口外才写一次 tokens.last_seen。
// 写库使用独立短超时的 context 并忽略错误：last_seen 不是关键数据，
// 绝不因为写它而拖慢或失败鉴权。调用方不阻塞在写锁上超过 tokenSeenTimeout。
func (d *DB) markTokenSeen(token string) {
	now := time.Now().Unix()
	interval := int64(tokenSeenInterval / time.Second)

	d.seenMu.Lock()
	if d.seen == nil {
		d.seen = make(map[string]int64, 64)
	}
	if len(d.seen) >= tokenSeenCap {
		// 令牌数量本不会这么多（重启后登录会刷新），满了直接清空最省事。
		d.seen = make(map[string]int64, tokenSeenCap)
	}
	if last, ok := d.seen[token]; ok && interval > 0 && now-last < interval {
		d.seenMu.Unlock()
		return
	}
	d.seen[token] = now
	d.seenMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), tokenSeenTimeout)
	defer cancel()
	_, _ = d.sql.ExecContext(ctx, `UPDATE tokens SET last_seen=? WHERE token=?`, now, token)
}

// ---------- 写入 ----------

// UpsertItems 批量写入扫描结果（幂等）。
func (d *DB) UpsertItems(ctx context.Context, items []model.Item, paths []PathRecord) error {
	if len(items) == 0 && len(paths) == 0 {
		return nil
	}
	defer markSearchDirty()
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 批量多行 INSERT：一次 100 行，把每行一条语句的往返开销摊薄。
	const chunk = 100
	for start := 0; start < len(items); start += chunk {
		end := start + chunk
		if end > len(items) {
			end = len(items)
		}
		part := items[start:end]
		var b strings.Builder
		b.WriteString("INSERT INTO items(" + itemInsertColumns + ") VALUES ")
		args := make([]any, 0, len(part)*30)
		for i := range part {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(itemPlaceholders)
			args = append(args, itemInsertArgs(&part[i])...)
		}
		b.WriteString(itemUpsertClause)
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return err
		}
		// 同步维护拼音索引：搜索依赖这两列，写入时算好，查询才能走 LIKE。
		idx, err := tx.PrepareContext(ctx, `UPDATE items SET pinyin=?, pinyin_initials=? WHERE id=?`)
		if err != nil {
			return err
		}
		for i := range part {
			full, initials := nameIndexes(part[i].Name)
			if _, err := idx.ExecContext(ctx, full, initials, part[i].Id); err != nil {
				idx.Close()
				return err
			}
		}
		idx.Close()

		// 先删本批条目的旧来源，再一次性插回，避免每条一次 DELETE。
		ids := make([]string, 0, len(part))
		var srcArgs []any
		var srcRows []string
		for i := range part {
			it := &part[i]
			if len(it.MediaSources) == 0 {
				continue
			}
			ids = append(ids, it.Id)
			for j, ms := range it.MediaSources {
				srcRows = append(srcRows, "(?,?,?,?,?,?,?)")
				srcArgs = append(srcArgs, it.Id, ms.Id, ms.Name, ms.Path, ms.Container, ms.RunTimeTicks, j)
			}
		}
		if len(ids) > 0 {
			ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
			delArgs := make([]any, len(ids))
			for i, id := range ids {
				delArgs[i] = id
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM media_sources WHERE item_id IN ("+ph+")", delArgs...); err != nil {
				return err
			}
			sql := "INSERT INTO media_sources(item_id,source_id,name,path,container,runtime_ticks,sort_order) VALUES " +
				strings.Join(srcRows, ",") +
				" ON CONFLICT(item_id,source_id) DO UPDATE SET name=excluded.name, path=excluded.path," +
				" container=excluded.container, runtime_ticks=excluded.runtime_ticks, sort_order=excluded.sort_order"
			if _, err := tx.ExecContext(ctx, sql, srcArgs...); err != nil {
				return err
			}
		}
	}

	if len(paths) > 0 {
		const pchunk = 200
		for start := 0; start < len(paths); start += pchunk {
			end := start + pchunk
			if end > len(paths) {
				end = len(paths)
			}
			part := paths[start:end]
			var b strings.Builder
			b.WriteString("INSERT INTO item_paths(path,item_id,mtime,size) VALUES ")
			args := make([]any, 0, len(part)*4)
			for i := range part {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString("(?,?,?,?)")
				args = append(args, part[i].Path, part[i].ItemId, part[i].MTime, part[i].Size)
			}
			b.WriteString(" ON CONFLICT(path) DO UPDATE SET item_id=excluded.item_id, mtime=excluded.mtime, size=excluded.size")
			if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (d *DB) UpsertViews(ctx context.Context, views []model.View) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range views {
		paths, _ := json.Marshal(v.Paths)
		if _, err := tx.ExecContext(ctx, `INSERT INTO views(id,name,collection_type,sort_order,paths)
			VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,
			collection_type=excluded.collection_type, sort_order=excluded.sort_order, paths=excluded.paths`,
			v.Id, v.Name, v.CollectionType, v.SortOrder, string(paths)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Stats 返回统计信息。
func (d *DB) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	rows, err := d.sql.QueryContext(ctx, `SELECT kind, count(*) FROM items GROUP BY kind`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			return s, err
		}
		s.Items += n
		switch kind {
		case "Movie":
			s.Movies = n
		case "Series":
			s.Series = n
		case "Episode":
			s.Episodes = n
		}
	}
	_ = d.sql.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&s.Users)
	if fi, err := os.Stat(d.path); err == nil {
		s.LibraryDBSize = fi.Size()
	}
	return s, rows.Err()
}

// ---------- 辅助 ----------

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func jsonCol(v any) string {
	if v == nil {
		return "[]"
	}
	switch t := v.(type) {
	case []string:
		if len(t) == 0 {
			return "[]"
		}
	case map[string]string:
		if len(t) == 0 {
			return "{}"
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func mapCol(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// SortedKeys 仅供调试使用。
func SortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ParseInt 便携解析。
func ParseInt(s string, def int) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return v
}

// ---------- 增量扫描 ----------

// ReadPathRecords 返回 path→记录 的快照，供扫描器跳过未变化文件。
// 26 万行约 12 MB 内存，扫描为期一次，可接受。
func (d *DB) ReadPathRecords(ctx context.Context) (map[string]PathRecord, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT path, item_id, mtime, size FROM item_paths`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]PathRecord, 1<<16)
	for rows.Next() {
		var p PathRecord
		if err := rows.Scan(&p.Path, &p.ItemId, &p.MTime, &p.Size); err != nil {
			return nil, err
		}
		out[p.Path] = p
	}
	return out, rows.Err()
}

// ---------- 刮削 ----------

// MetaPatch 是一次刮削要写回的字段集合。
// 零值表示“不改”：overview/sort 等字符串为空时保持原值，
// slices/maps 为 nil 时保持原值；布尔 NeedRescrape 由调用方用 ScrapeMark 控制。
type MetaPatch struct {
	Id              string
	Name            string
	OriginalTitle   string
	SortName        string
	ForcedSortName  string
	Overview        string
	PremiereDate    string
	ProductionYear  int
	OfficialRating  string
	CommunityRating float64
	CriticRating    float64
	RunTimeTicks    int64
	Genres          []string
	Tags            []string
	Studios         []model.NameId
	People          []model.Person
	ProviderIds     map[string]string
	Taglines        []string
	ImageRef        string
	BackdropRef     string
	LogoRef         string
	StillPath       string // TMDB still_path，按需下载
	MediaStreams    []model.MediaStream
}

// ScrapeMark 是一次刮削的结果记录。
type ScrapeMark struct {
	ItemId string
	Status string // ok | notfound | failed | skipped
	Source string // tmdb | nfo | local
	Error  string
}

// ApplyMeta 写入刮削元数据并记录 scrape_state（同一事务）。
// 只更新 patch 中“有值”的列；空字符串/零值不动。
func (d *DB) ApplyMeta(ctx context.Context, p MetaPatch, m ScrapeMark) error {
	defer markSearchDirty()
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	upd, err := tx.ExecContext(ctx, `UPDATE items SET
		name              = CASE WHEN ?<>'' THEN ? ELSE name END,
		original_title    = CASE WHEN ?<>'' THEN ? ELSE original_title END,
		sort_name         = CASE WHEN ?<>'' THEN ? ELSE sort_name END,
		forced_sort_name  = CASE WHEN ?<>'' THEN ? ELSE forced_sort_name END,
		overview          = CASE WHEN ?<>'' THEN ? ELSE overview END,
		premiere_date     = CASE WHEN ?<>'' THEN ? ELSE premiere_date END,
		year              = CASE WHEN ?>0   THEN ? ELSE year END,
		official_rating   = CASE WHEN ?<>'' THEN ? ELSE official_rating END,
		community_rating  = CASE WHEN ?>0   THEN ? ELSE community_rating END,
		critic_rating     = CASE WHEN ?>0   THEN ? ELSE critic_rating END,
		runtime_ticks     = CASE WHEN ?>0   THEN ? ELSE runtime_ticks END,
		genres            = CASE WHEN ?<>'' THEN ? ELSE genres END,
		tags              = CASE WHEN ?<>'' THEN ? ELSE tags END,
		studios           = CASE WHEN ?<>'' THEN ? ELSE studios END,
		people            = CASE WHEN ?<>'' THEN ? ELSE people END,
		provider_ids      = CASE WHEN ?<>'' THEN ? ELSE provider_ids END,
		taglines          = CASE WHEN ?<>'' THEN ? ELSE taglines END,
		image_ref         = CASE WHEN ?<>'' THEN ? ELSE image_ref END,
		tmdb_still        = CASE WHEN ?<>'' THEN ? ELSE tmdb_still END,
		backdrop_ref      = CASE WHEN ?<>'' THEN ? ELSE backdrop_ref END,
		logo_ref          = CASE WHEN ?<>'' THEN ? ELSE logo_ref END,
		scraped_at        = ?
		WHERE id = ?`,
		p.Name, p.Name,
		p.OriginalTitle, p.OriginalTitle,
		p.SortName, p.SortName,
		p.ForcedSortName, p.ForcedSortName,
		p.Overview, p.Overview,
		p.PremiereDate, p.PremiereDate,
		p.ProductionYear, p.ProductionYear,
		p.OfficialRating, p.OfficialRating,
		p.CommunityRating, p.CommunityRating,
		p.CriticRating, p.CriticRating,
		p.RunTimeTicks, p.RunTimeTicks,
		joinColIf(p.Genres), joinColIf(p.Genres),
		joinColIf(p.Tags), joinColIf(p.Tags),
		jsonIf(p.Studios), jsonIf(p.Studios),
		jsonIf(p.People), jsonIf(p.People),
		mapIf(p.ProviderIds), mapIf(p.ProviderIds),
		joinColIf(p.Taglines), joinColIf(p.Taglines),
		p.ImageRef, p.ImageRef,
		p.StillPath, p.StillPath,
		p.BackdropRef, p.BackdropRef,
		p.LogoRef, p.LogoRef,
		time.Now().Unix(), p.Id)
	if err != nil {
		return err
	}
	// 0 行说明条目已不存在（被删除/ID 变更）：此时绝不能写 scrape_state，
	// 否则会留下永远指向不存在条目的孤儿状态。
	n, err := upd.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("ApplyMeta %s: %w", p.Id, ErrNotFound)
	}

	if m.Status != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO scrape_state(item_id,status,source,attempts,error,updated)
			VALUES(?,?,?,1,?,?)
			ON CONFLICT(item_id) DO UPDATE SET status=excluded.status, source=excluded.source,
			attempts=scrape_state.attempts+1, error=excluded.error, updated=excluded.updated`,
			m.ItemId, m.Status, m.Source, m.Error, time.Now().Unix()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ScrapeCandidate 是需要刮削的条目（只读投影，避免拉全列）。
type ScrapeCandidate struct {
	Id          string
	Type        string
	Name        string
	Year        int
	Path        string
	ParentId    string
	SeriesName  string
	SeriesTmdb  string
	SeasonNum   int
	EpisodeNum  int
	ProviderIds map[string]string
	HasOverview bool
	// HasEpisodeInfo：Episode 的季/集号可信（扫描器解析出 SxxExx 或第N集）。
	HasEpisodeInfo bool
}

// scrapeKinds 是参与刮削的条目类型。
// 单集不再作为独立刮削对象：剧集刮削时通过 TMDB 季详情批量回填，
// 否则会按“集名”去搜索，容易匹配到别的剧，造成数据错乱。
const scrapeKinds = `('Movie','Series')`

// kindFilter 把库类型映射成 SQL 的 kind 白名单。
// tvshows 只取 Series（一部剧算一条），movies 只取 Movie；空/未知则两类都取。
func kindFilter(kinds []string) string {
	if len(kinds) == 0 {
		return scrapeKinds
	}
	seen := map[string]bool{}
	parts := make([]string, 0, 2)
	for _, k := range kinds {
		switch k {
		case "Movie", "Series":
			if !seen[k] {
				seen[k] = true
				parts = append(parts, "'"+k+"'")
			}
		}
	}
	if len(parts) == 0 {
		return scrapeKinds
	}
	return "(" + strings.Join(parts, ",") + ")"
}

// DefaultMaxScrapeAttempts 是候选过滤里 attempts 的默认上限，
// 与 scraper.Options.MaxAttempts 的默认值保持一致。
const DefaultMaxScrapeAttempts = 5

// scrapeWhere 返回候选条目的公共 WHERE 片段与参数。
// force=true：全量刮削，所有条目都算候选（忽略 scrape_state 与锁定）。
// force=false：增量刮削，只挑“从未刮削 / 明确失败可重试 / 已刮削但元数据缺失”的条目；
// 已有完整元数据且未变更的条目直接跳过，不做按时间的老化重刮。
//
// maxAttempts>0 时，增量模式会排除 attempts 已达上限的条目（失败隔离）：
// 这些条目必须在 SQL 里过滤掉，否则会占满每批窗口，让 Count 永远完不成、进度停在 0。
// force 模式按定义“忽略 scrape_state”，因此不做 attempts 过滤。
func scrapeWhere(force bool, paths []string, kinds []string, maxAttempts int) (string, []any) {
	var b strings.Builder
	var args []any
	b.WriteString(" WHERE i.kind IN " + kindFilter(kinds))
	if len(paths) > 0 {
		// 一个媒体库可以配置多个根路径（如 欧美剧集 = 外语+英语+限制级）。
		b.WriteString(" AND (")
		for i, p := range paths {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("i.path LIKE ?")
			args = append(args, strings.TrimRight(strings.TrimSpace(p), "/")+"%")
		}
		b.WriteString(")")
	}
	if force {
		b.WriteString(" AND 1=1")
	} else {
		b.WriteString(` AND COALESCE(i.locked_fields,'[]') IN ('','[]')`)
		// “缺少元数据”按“正文和海报都为空”判定：只缺海报（TMDB 本来就没有）
		// 不反复重刮，避免每轮任务都处理同一批条目。
		b.WriteString(` AND (s.item_id IS NULL OR s.status IN ('notfound','failed') OR ` +
			`(s.status='ok' AND COALESCE(i.overview,'')='' AND COALESCE(i.image_ref,'')=''))`)
		if maxAttempts <= 0 {
			maxAttempts = DefaultMaxScrapeAttempts
		}
		b.WriteString(" AND COALESCE(s.attempts,0) < ?")
		args = append(args, maxAttempts)
	}
	return b.String(), args
}

// ListScrapeCandidates 返回需要刮削的条目（force=全量，否则增量）。
//
// 分页方式：按 i.id 升序做游标（afterId），保证每一轮都能向前推进、
// 既不会重复取到同一批（force 模式忽略 scrape_state，靠 SQL 游标而不是内存去重），
// 也不会无限循环。批内再按业务优先级排序：发行日倒序，新剧先刮；同日按类型（Series 先于 Movie）。
func (d *DB) ListScrapeCandidates(ctx context.Context, limit int, force bool, paths []string, kinds []string, afterId string, maxAttempts int) ([]ScrapeCandidate, error) {
	if limit <= 0 {
		limit = 500
	}
	// provider 投影：父剧的 tmdb id 用于拼 episode 的详情请求（省一次搜索）。
	hint := `CASE WHEN i.kind='Episode' THEN COALESCE(parent.provider_ids,'{}') ELSE '{}' END`
	hasEpInfo := `CASE WHEN i.kind='Episode' THEN
		CASE WHEN COALESCE(i.episode_number,0)>0 AND COALESCE(i.season_number,0)>=0 THEN 1 ELSE 0 END
		ELSE 1 END`
	where, args := scrapeWhere(force, paths, kinds, maxAttempts)
	// 内层：用 id 游标取下一批（稳定、单调、不会漏也不会重复）。
	inner := `SELECT i.id AS id, i.kind AS kind, i.name AS name, COALESCE(i.year,0) AS year,
		COALESCE(i.path,'') AS path, i.parent_id AS parent_id, COALESCE(i.series_name,'') AS series_name,
		` + hint + ` AS hint, ` + hasEpInfo + ` AS has_ep_info,
		COALESCE(i.season_number,0) AS season_number, COALESCE(i.episode_number,0) AS episode_number,
		COALESCE(i.provider_ids,'{}') AS provider_ids,
		CASE WHEN COALESCE(i.overview,'')<>'' THEN 1 ELSE 0 END AS has_overview,
		COALESCE(NULLIF(i.premiere_date,''), printf('%04d-01-01', COALESCE(i.year,0))) AS premiere,
		CASE i.kind WHEN 'Series' THEN 0 WHEN 'Movie' THEN 1 ELSE 2 END AS kind_rank,
		COALESCE(s.attempts,0) AS attempts
		FROM items i
		LEFT JOIN items parent ON parent.id = i.series_id
		LEFT JOIN scrape_state s ON s.item_id = i.id` + where + `
		AND ` + hasEpInfo + `=1
		AND i.id > ?
		ORDER BY i.id ASC LIMIT ?`
	rowsSQL := `SELECT id, kind, name, year, path, parent_id, series_name, hint, has_ep_info,
		season_number, episode_number, provider_ids, has_overview
		FROM (` + inner + `)
		ORDER BY premiere DESC, kind_rank ASC, attempts ASC, id ASC`
	args = append(args, afterId, limit)
	rows, err := d.sql.QueryContext(ctx, rowsSQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ScrapeCandidate, 0, limit)
	for rows.Next() {
		var c ScrapeCandidate
		var hintJSON, provJSON string
		var hasOverview, hasEpInfo int
		if err := rows.Scan(&c.Id, &c.Type, &c.Name, &c.Year, &c.Path, &c.ParentId,
			&c.SeriesName, &hintJSON, &hasEpInfo, &c.SeasonNum, &c.EpisodeNum, &provJSON, &hasOverview); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(provJSON), &c.ProviderIds)
		if c.Type == "Episode" && hintJSON != "" {
			var m map[string]string
			_ = json.Unmarshal([]byte(hintJSON), &m)
			c.SeriesTmdb = m["tmdb"]
		}
		c.HasOverview = hasOverview == 1
		c.HasEpisodeInfo = hasEpInfo == 1
		out = append(out, c)
	}
	return out, rows.Err()
}

// CountScrapeCandidates 返回与 ListScrapeCandidates 相同口径的候选总数，
// 用于任务中心展示“总数”，保证两者一致（含同一套 attempts 过滤）。
func (d *DB) CountScrapeCandidates(ctx context.Context, force bool, paths []string, kinds []string, maxAttempts int) (int, error) {
	where, args := scrapeWhere(force, paths, kinds, maxAttempts)
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT count(*) FROM items i
		LEFT JOIN scrape_state s ON s.item_id = i.id`+where, args...).Scan(&n)
	return n, err
}

// ScrapeStats 汇总 scrape_state。
func (d *DB) ScrapeStats(ctx context.Context) (map[string]int, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT status, count(*) FROM scrape_state GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

// PendingScrapes 计数器：待刮削条目数（用于进度日志）。
func (d *DB) PendingScrapes(ctx context.Context) (int, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT count(*) FROM items i
		LEFT JOIN scrape_state s ON s.item_id = i.id
		WHERE i.kind IN `+scrapeKinds+` AND
		(s.item_id IS NULL OR (s.status<>'ok' AND s.attempts < ?))`, DefaultMaxScrapeAttempts).Scan(&n)
	return n, err
}

// joinColIf 返回 JSON 列或空串：空串表示“本次不修改该列”（CASE WHEN ?<>”）。
func joinColIf(v []string) string {
	if v == nil {
		return ""
	}
	return jsonCol(v)
}

func jsonIf(v any) string {
	if v == nil {
		return ""
	}
	return jsonCol(v)
}

func mapIf(m map[string]string) string {
	if m == nil {
		return ""
	}
	return mapCol(m)
}

// ScrapeAttempts 返回条目已尝试刮削次数（供 scraper 做失败隔离）。
// 注意：逐条查询在 26 万条目下会拖慢扫描；实际调用时应按批次预取。
func (d *DB) ScrapeAttempts(id string) int {
	var n int
	_ = d.sql.QueryRow(`SELECT attempts FROM scrape_state WHERE item_id=?`, id).Scan(&n)
	return n
}

// ScrapeAttemptsBatch 预取一批条目的 attempts（scraper 每批调用一次）。
func (d *DB) ScrapeAttemptsBatch(ctx context.Context, ids []string) (map[string]int, error) {
	out := make(map[string]int, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	ph := strings.Repeat("?,", len(ids)-1) + "?"
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := d.sql.QueryContext(ctx, `SELECT item_id, attempts FROM scrape_state WHERE item_id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// ResetScrapeState 清空刮削记录（-force 时先把 attempts 清零，让失败条目重新进入队列）。
func (d *DB) ResetScrapeState(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM scrape_state`)
	return err
}

// View 是媒体库路径配置（Library/VirtualFolders）。
// 目前每个 view 对应 MediaRoot 下的一个子目录；没有子目录时使用 MediaRoot 本身。
func (d *DB) UpsertLibraryPath(ctx context.Context, viewId string, sortOrder int) error {
	// 只建 view 行；路径由 MediaRoot 决定（单库根目录）。
	_, err := d.sql.ExecContext(ctx, `INSERT INTO views(id,name,collection_type,path,sort_order)
		VALUES(?,?,'movies',?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,
		collection_type=excluded.collection_type, path=excluded.path, sort_order=excluded.sort_order`,
		viewId, viewId, nil, sortOrder)
	return err
}

// MarkScanned 在一次扫描结束时回收已消失的条目与路径。
// 传入本次见到的 item id 集合不现实（26 万 × 32 字节约 8 MB 可用，但 SQL 无法一次用完）；
// 改成按路径：item_paths 里未在本次扫描中出现的 path 删除，并删掉没有子项、没有 path 的容器。
func (d *DB) PruneMissingPaths(ctx context.Context, seen []string) (int, error) {
	if len(seen) == 0 {
		return 0, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS scan_seen(path TEXT PRIMARY KEY)`); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM scan_seen`); err != nil {
		return 0, err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO scan_seen(path) VALUES(?)`)
	if err != nil {
		return 0, err
	}
	for _, p := range seen {
		if _, err := stmt.ExecContext(ctx, p); err != nil {
			stmt.Close()
			return 0, err
		}
	}
	stmt.Close()

	// 1) 删 path 记录，返回受影响的 item 集合
	rows, err := tx.QueryContext(ctx, `SELECT item_id FROM item_paths WHERE path NOT IN (SELECT path FROM scan_seen)`)
	if err != nil {
		return 0, err
	}
	var stale []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, id)
	}
	rows.Close()

	if _, err := tx.ExecContext(ctx, `DELETE FROM item_paths WHERE path NOT IN (SELECT path FROM scan_seen)`); err != nil {
		return 0, err
	}
	if len(stale) > 0 {
		ph := strings.Repeat("?,", len(stale)-1) + "?"
		args := make([]any, len(stale))
		for i, id := range stale {
			args[i] = id
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id IN (`+ph+`)`, args...); err != nil {
			return 0, err
		}
	}
	// 2) 清理因此变成空壳的 Series/Season（没有子项、没有 path 记录）。
	if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE kind IN ('Season','Series','Folder')
		AND NOT EXISTS (SELECT 1 FROM items c WHERE c.parent_id = items.id)`); err != nil {
		return 0, err
	}
	return len(stale), tx.Commit()
}

// DeletePathPrefix 删除某个已消失目录下的全部媒体路径（标题目录整目录删除时使用）。
// relPrefix 是相对 BaseRoot 的路径；只在该前缀范围内删除，绝不接受空前缀。
func (d *DB) DeletePathPrefix(ctx context.Context, relPrefix, baseRoot string) (int, error) {
	prefix := strings.Trim(filepath.ToSlash(strings.TrimSpace(relPrefix)), "/")
	if prefix == "" || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return 0, errors.New("store: invalid delete prefix")
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT path FROM item_paths WHERE path=? OR path LIKE ?`, prefix, prefix+"/%")
	if err != nil {
		return 0, err
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return 0, err
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	for _, p := range paths {
		abs := filepath.Join(baseRoot, filepath.FromSlash(p))
		if _, err := d.DeletePath(ctx, p, abs); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// DeletePath 删除一个已消失的媒体文件路径：移除对应 media_sources/item_paths；
// 只有该 item 已无任何其它来源时才删除 item，并回收空的 Season/Series。
// 返回 false 表示该路径没有数据库记录（幂等）。
// 调用方（扫描器）必须把 path 限定在本次扫描根之下，绝不应对全库调用。
func (d *DB) DeletePath(ctx context.Context, path, absPath string) (bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return false, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT item_id FROM item_paths WHERE path=?`, path)
	if err != nil {
		return false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(ids) == 0 {
		return false, nil
	}

	// media_sources.path 存的是本地绝对路径，item_paths.path 是相对 BaseRoot 的路径；
	// 只删这条来源对应的绝对路径，且限定在该 item 内，避免误删同名的其它来源。
	if absPath != "" {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		args := make([]any, 0, len(ids)+1)
		args = append(args, absPath)
		for _, id := range ids {
			args = append(args, id)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM media_sources WHERE path=? AND item_id IN (`+ph+`)`, args...); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM item_paths WHERE path=?`, path); err != nil {
		return false, err
	}

	// 记录被删条目的父级，随后只沿着这些父级向上回收空壳；
	// 绝不能全局 DELETE 空的 Season/Series（扫一个库会波及其它库）。
	var parents []string
	for _, id := range ids {
		var parent, series string
		if err := tx.QueryRowContext(ctx, `SELECT coalesce(parent_id,''), coalesce(series_id,'') FROM items WHERE id=?`, id).Scan(&parent, &series); err == nil {
			if parent != "" {
				parents = append(parents, parent)
			}
			if series != "" {
				parents = append(parents, series)
			}
		}
	}
	for _, id := range ids {
		var paths, sources int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM item_paths WHERE item_id=?`, id).Scan(&paths); err != nil {
			return false, err
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM media_sources WHERE item_id=?`, id).Scan(&sources); err != nil {
			return false, err
		}
		if paths > 0 || sources > 0 {
			continue
		}
		for _, q := range []string{
			`DELETE FROM user_data WHERE item_id=?`,
			`DELETE FROM scrape_state WHERE item_id=?`,
			`DELETE FROM items WHERE id=?`,
		} {
			if _, err := tx.ExecContext(ctx, q, id); err != nil {
				return false, err
			}
		}
	}

	// 只从受影响父级向上回收空壳（Season → Series），不碰其它库。
	seenParents := map[string]bool{}
	for len(parents) > 0 {
		id := parents[len(parents)-1]
		parents = parents[:len(parents)-1]
		if id == "" || seenParents[id] {
			continue
		}
		var kind, parent string
		err := tx.QueryRowContext(ctx, `SELECT kind, coalesce(parent_id,'') FROM items WHERE id=?`, id).Scan(&kind, &parent)
		if err != nil {
			continue
		}
		switch kind {
		case "Season", "Series", "Folder":
		default:
			continue
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE parent_id=?`, id).Scan(&n); err != nil {
			return false, err
		}
		if n != 0 {
			// 先遇到的父级可能还在等待子级删除（栈顺序），不能提前标记为已见，
			// 否则子级删除后父级不会被重新检查。只有真正删掉才记入 seen。
			continue
		}
		seenParents[id] = true
		if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, id); err != nil {
			return false, err
		}
		if parent != "" {
			parents = append(parents, parent)
		}
	}
	return true, tx.Commit()
}

// DeleteUser 删除账号及其令牌与用户数据；返回删除的账号行数。
func (d *DB) DeleteUser(ctx context.Context, id string) (int64, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM tokens WHERE user_id=?`, id); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_data WHERE user_id=?`, id); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM users WHERE id=?`, id)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
}

// ListUsers 返回全部账号（含管理员标志），按创建时间排序。
func (d *DB) ListUsers(ctx context.Context) ([]model.User, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, name, password_hash, is_admin,
		        coalesce(is_disabled,0), coalesce(max_active_sessions,0),
		        coalesce(enable_all_folders,1), coalesce(enabled_folders,'[]'), coalesce(blocked_tags,'[]'),
		        coalesce(avatar_ref,'')
		 FROM users ORDER BY created, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.User
	for rows.Next() {
		var u model.User
		var hash string
		var admin, disabled, maxSessions, allFolders int
		var foldersJSON, tagsJSON sql.NullString
		var avatarRef string
		if err := rows.Scan(&u.Id, &u.Name, &hash, &admin, &disabled, &maxSessions, &allFolders, &foldersJSON, &tagsJSON, &avatarRef); err != nil {
			return nil, err
		}
		u.HasPassword = hash != ""
		u.IsAdmin = admin == 1
		if avatarRef != "" {
			u.PrimaryImageTag = imageVersionTag(avatarRef)
		}
		applyUserPolicy(&u, disabled, maxSessions, allFolders, foldersJSON.String, tagsJSON.String)
		out = append(out, u)
	}
	return out, rows.Err()
}

// UpdateUserPolicy 写入用户策略。
func (d *DB) UpdateUserPolicy(ctx context.Context, id string, p model.UserPolicy) error {
	folders, _ := json.Marshal(p.EnabledFolders)
	tags, _ := json.Marshal(p.BlockedTags)
	all := 0
	if p.EnableAllFolders {
		all = 1
	}
	admin := 0
	if p.IsAdministrator {
		admin = 1
	}
	disabled := 0
	if p.IsDisabled {
		disabled = 1
	}
	_, err := d.sql.ExecContext(ctx, `UPDATE users SET is_admin=?, is_disabled=?,
		max_active_sessions=?, enable_all_folders=?, enabled_folders=?, blocked_tags=? WHERE id=?`,
		admin, disabled, p.MaxActiveSessions, all, string(folders), string(tags), id)
	return err
}

// SetUserPassword 重设密码（空字符串 = 清除密码）。
func (d *DB) SetUserPassword(ctx context.Context, id, password string) error {
	hash := ""
	if password != "" {
		hash = embyPasswordHash(password)
	}
	_, err := d.sql.ExecContext(ctx, `UPDATE users SET password_hash=? WHERE id=?`, hash, id)
	return err
}

// UserByID 按 ID 取用户（含策略）。
func (d *DB) UserByID(ctx context.Context, id string) (*model.User, error) {
	var u model.User
	var hash string
	var admin, disabled, maxSessions, allFolders int
	var foldersJSON, tagsJSON sql.NullString
	var avatarRef string
	err := d.sql.QueryRowContext(ctx, `SELECT id, name, password_hash, is_admin,
		coalesce(is_disabled,0), coalesce(max_active_sessions,0),
		coalesce(enable_all_folders,1), coalesce(enabled_folders,'[]'), coalesce(blocked_tags,'[]'),
		coalesce(avatar_ref,'')
		FROM users WHERE id=?`, id).
		Scan(&u.Id, &u.Name, &hash, &admin, &disabled, &maxSessions, &allFolders, &foldersJSON, &tagsJSON, &avatarRef)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	u.HasPassword = hash != ""
	u.IsAdmin = admin == 1
	if avatarRef != "" {
		u.PrimaryImageTag = imageVersionTag(avatarRef)
	}
	applyUserPolicy(&u, disabled, maxSessions, allFolders, foldersJSON.String, tagsJSON.String)
	return &u, nil
}

// UpdateSeasonPoster 给某剧集下的指定季写海报引用。
func (d *DB) UpdateSeasonPoster(ctx context.Context, seriesItemID string, seasonNumber int, ref string) error {
	_, err := d.sql.ExecContext(ctx,
		`UPDATE items SET image_ref=? WHERE kind='Season' AND series_id=? AND season_number=?`,
		ref, seriesItemID, seasonNumber)
	return err
}

// ItemEdit 是手动编辑元数据的补丁（只允许白名单字段）。
type ItemEdit struct {
	Name           *string
	Overview       *string
	OfficialRating *string
	ProductionYear *int
	Genres         []string
	Tags           []string
	ProviderIds    map[string]string
	Locked         []string
}

// UpdateItemMetadata 手动修改条目元数据；Locked 非空表示锁定，后续自动刮削会跳过。
func (d *DB) UpdateItemMetadata(ctx context.Context, id string, e ItemEdit) error {
	sets := []string{}
	args := []any{}
	if e.Name != nil {
		sets = append(sets, "name=?", "sort_name=?")
		args = append(args, *e.Name, *e.Name)
	}
	if e.Overview != nil {
		sets = append(sets, "overview=?")
		args = append(args, *e.Overview)
	}
	if e.OfficialRating != nil {
		sets = append(sets, "official_rating=?")
		args = append(args, *e.OfficialRating)
	}
	if e.ProductionYear != nil {
		sets = append(sets, "year=?")
		args = append(args, *e.ProductionYear)
	}
	if e.Genres != nil {
		b, _ := json.Marshal(e.Genres)
		sets = append(sets, "genres=?")
		args = append(args, string(b))
	}
	if e.Tags != nil {
		b, _ := json.Marshal(e.Tags)
		sets = append(sets, "tags=?")
		args = append(args, string(b))
	}
	if e.ProviderIds != nil {
		b, _ := json.Marshal(e.ProviderIds)
		sets = append(sets, "provider_ids=?")
		args = append(args, string(b))
	}
	if e.Locked != nil {
		b, _ := json.Marshal(e.Locked)
		sets = append(sets, "locked_fields=?")
		args = append(args, string(b))
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	_, err := d.sql.ExecContext(ctx, "UPDATE items SET "+strings.Join(sets, ", ")+" WHERE id=?", args...)
	return err
}

// ResumeItems 返回用户「继续观看」：有进度、未看完的条目。
func (d *DB) ResumeItems(ctx context.Context, userID string, limit int) ([]model.Item, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	// 只看“看了一半”的：剩下的不足 10% 视为看完，不进继续观看。
	// 排序必须按“最后播放/进度更新时间倒序”——之前的写法在外层又按 date_created 排序，
	// 把子查询的顺序彻底覆盖掉，导致刚看的片排在很早入库的片后面。
	q := `SELECT ` + itemColumns + ` FROM items WHERE id IN (
		SELECT item_id FROM user_data
		WHERE user_id=? AND played=0 AND position_ticks>0
		  AND (duration_ticks=0 OR position_ticks < duration_ticks*9/10)
		ORDER BY coalesce(updated,0) DESC, coalesce(last_played,0) DESC LIMIT ?)
		ORDER BY (SELECT coalesce(ud.updated, ud.last_played*1000, 0)
		          FROM user_data ud WHERE ud.user_id=? AND ud.item_id=items.id) DESC`
	items, err := d.queryItemsList(ctx, q, userID, limit, userID)
	if err != nil {
		return nil, err
	}
	// 客户端靠 UserData.PlaybackPositionTicks 画进度条，必须补上。
	if err := d.attachUserData(ctx, userID, items); err != nil {
		return nil, err
	}
	return items, nil
}

// LatestItems 返回“最新媒体”。parentID 是媒体库时按库类型返回顶层条目
// （电影库→Movie，剧集库→Series）；不传则返回全库的 Movie/Series。
// 客户端（Moonfin 等）用它渲染首页“最新”栏目，返回顶层条目才不会因为 Episode 无法展示而留空。
func (d *DB) LatestItems(ctx context.Context, parentID string, limit int) ([]model.Item, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	kinds := []string{"Movie", "Series"}
	if parentID != "" {
		var ct string
		if err := d.sql.QueryRowContext(ctx, `SELECT coalesce(collection_type,'') FROM views WHERE id=?`, parentID).Scan(&ct); err == nil {
			if ct == "movies" {
				kinds = []string{"Movie"}
			} else if ct == "tvshows" {
				kinds = []string{"Series"}
			}
		}
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(kinds)), ",")
	args := make([]any, 0, len(kinds)+2)
	for _, k := range kinds {
		args = append(args, k)
	}
	where := "kind IN (" + ph + ")"
	if parentID != "" {
		where += " AND parent_id=?"
		args = append(args, parentID)
	} else {
		// 不指定库时也只返回已添加媒体库中的条目，避免未归属路径混进首页。
		where += " AND parent_id IN (SELECT id FROM views)"
	}
	// 两段式：先用覆盖索引取 20 个 ID（避免对整张 12k 行表做宽行排序），再回表。
	idSQL := `SELECT id FROM items WHERE ` + where + `
		ORDER BY premiere_date DESC, id DESC LIMIT ?`
	args = append(args, limit)
	idRows, err := d.sql.QueryContext(ctx, idSQL, args...)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, limit)
	for idRows.Next() {
		var id string
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	idRows.Close()
	if err := idRows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []model.Item{}, nil
	}
	phIDs := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	idArgs := make([]any, len(ids))
	for i, id := range ids {
		idArgs[i] = id
	}
	list, err := d.queryItemsList(ctx, `SELECT `+itemColumns+` FROM items WHERE id IN (`+phIDs+`)`, idArgs...)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]model.Item, len(list))
	for _, it := range list {
		byID[it.Id] = it
	}
	out := make([]model.Item, 0, len(ids))
	for _, id := range ids {
		if it, ok := byID[id]; ok {
			out = append(out, it)
		}
	}
	return out, nil
}

func (d *DB) queryItemsList(ctx context.Context, q string, args ...any) ([]model.Item, error) {
	rows, err := d.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Item
	for rows.Next() {
		it, err := scanItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// TouchItem 用于手动刷新：写入 scraped_at=0 让下一轮刮削重新处理。
func (d *DB) TouchItem(ctx context.Context, id string) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE items SET scraped_at=0 WHERE id=?`, id)
	return err
}

// BulkBegin 进入批量写模式：关闭同步刷盘、加大临时表内存，用于一次性入库。
// 必须在 BulkEnd 之前完成所有写入，否则崩溃时可能丢数据。
func (d *DB) BulkBegin(ctx context.Context) error {
	for _, p := range []string{
		// 不用 synchronous=OFF：脏页会堆积到最后一次性回写，整机 IO 被拖死。
		"PRAGMA synchronous=NORMAL",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA cache_size=-8192", // 8MB：小步刷新，避免一次性倒脏页
		"PRAGMA wal_autocheckpoint=256",
		"PRAGMA busy_timeout=30000",
	} {
		if _, err := d.sql.ExecContext(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

// BulkEnd 恢复安全设置并做一次 WAL checkpoint，避免 WAL 无限增长。
func (d *DB) BulkEnd(ctx context.Context) error {
	for _, p := range []string{"PRAGMA synchronous=NORMAL"} {
		if _, err := d.sql.ExecContext(ctx, p); err != nil {
			return err
		}
	}
	_, err := d.sql.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// itemInsertColumns / itemPlaceholders / itemUpsertClause 供多行 INSERT 复用。
const itemInsertColumns = `id, kind, name, original_title, sort_name, parent_id, series_id, series_name, season_id,
	season_number, episode_number, year, runtime_ticks, overview, official_rating,
	community_rating, path, container, tmdb_id, is_folder, genres, image_tags,
	backdrop_tags, provider_ids, date_created, sort_index,
	premiere_date, studios, image_ref, backdrop_ref, forced_sort_name`

const itemPlaceholders = "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

const itemUpsertClause = `
	ON CONFLICT(id) DO UPDATE SET
		kind=excluded.kind, name=excluded.name, sort_name=excluded.sort_name,
		parent_id=excluded.parent_id, series_id=excluded.series_id,
		series_name=CASE WHEN excluded.series_name<>'' THEN excluded.series_name ELSE items.series_name END,
		season_id=excluded.season_id,
		season_number=excluded.season_number, episode_number=excluded.episode_number,
		year=CASE WHEN excluded.year<>0 THEN excluded.year ELSE items.year END,
		runtime_ticks=CASE WHEN excluded.runtime_ticks<>0 THEN excluded.runtime_ticks ELSE items.runtime_ticks END,
		overview=CASE WHEN excluded.overview<>'' THEN excluded.overview ELSE items.overview END,
		official_rating=CASE WHEN excluded.official_rating<>'' THEN excluded.official_rating ELSE items.official_rating END,
		community_rating=CASE WHEN excluded.community_rating<>0 THEN excluded.community_rating ELSE items.community_rating END,
		path=excluded.path, container=excluded.container, is_folder=excluded.is_folder,
		genres=CASE WHEN excluded.genres IS NOT NULL THEN excluded.genres ELSE items.genres END,
		image_tags=excluded.image_tags, backdrop_tags=excluded.backdrop_tags,
		provider_ids=CASE WHEN excluded.provider_ids<>'{}' THEN excluded.provider_ids ELSE items.provider_ids END,
		date_created=items.date_created,
		sort_index=excluded.sort_index,
		premiere_date=CASE WHEN excluded.premiere_date<>'' THEN excluded.premiere_date ELSE items.premiere_date END,
		studios=CASE WHEN excluded.studios<>'[]' THEN excluded.studios ELSE items.studios END,
		image_ref=CASE WHEN excluded.image_ref<>'' THEN excluded.image_ref ELSE items.image_ref END,
		backdrop_ref=CASE WHEN excluded.backdrop_ref<>'' THEN excluded.backdrop_ref ELSE items.backdrop_ref END,
		forced_sort_name=CASE WHEN excluded.forced_sort_name<>'' THEN excluded.forced_sort_name ELSE items.forced_sort_name END`

// itemInsertArgs 按 itemInsertColumns 的顺序展开参数。
func itemInsertArgs(it *model.Item) []any {
	sortName := it.SortName
	if sortName == "" {
		sortName = it.Name
	}
	created := time.Now().Unix()
	if it.DateCreated != "" {
		if t, err := time.Parse(time.RFC3339, it.DateCreated); err == nil {
			created = t.Unix()
		}
	}
	var genres any
	if len(it.Genres) > 0 {
		genres = jsonCol(it.Genres)
	}
	imageTags := mapCol(it.ImageTags)
	backdrop := jsonCol(it.BackdropImageTags)
	providers := mapCol(it.ProviderIds)
	studios := jsonCol(it.Studios)
	isFolder := 0
	if it.IsFolder {
		isFolder = 1
	}
	tmdb := ""
	if providers != "{}" {
		tmdb = it.ProviderIds["tmdb"]
	}
	return []any{it.Id, it.Type, it.Name, it.OriginalTitle, sortName,
		it.ParentId, it.SeriesId, it.SeriesName, it.SeasonId,
		it.ParentIndexNumber, it.IndexNumber, it.ProductionYear, it.RunTimeTicks,
		it.Overview, it.OfficialRating, it.CommunityRating, nullStr(it.Path),
		nullStr(it.Container), nullStr(tmdb), isFolder, genres, imageTags, backdrop, providers,
		created, it.IndexNumber,
		it.PremiereDate, studios, nullStr(it.ImageRef), nullStr(it.BackdropRef), it.ForcedSortName}
}

// PathEntry 是增量扫描用的路径索引条目。
type PathEntry struct {
	MTime  int64
	Size   int64
	ItemId string
}

// LoadPathIndexUnder 只载入指定相对目录（含后代）的路径索引，用于 Pansave 定向扫描。
// 避免每次通知都为 15 万条路径建一张全量 map。
func (d *DB) LoadPathIndexUnder(ctx context.Context, prefixes []string) (map[string]PathEntry, error) {
	clean := make([]string, 0, len(prefixes))
	seen := map[string]bool{}
	for _, p := range prefixes {
		p = strings.Trim(filepath.ToSlash(strings.TrimSpace(p)), "/")
		if p == "" || p == "." || seen[p] {
			continue
		}
		seen[p] = true
		clean = append(clean, p)
	}
	if len(clean) == 0 {
		return map[string]PathEntry{}, nil
	}
	var conds []string
	args := make([]any, 0, len(clean)*2)
	for _, p := range clean {
		conds = append(conds, "path=?", "path LIKE ?")
		args = append(args, p, p+"/%")
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT path, mtime, size, item_id FROM item_paths WHERE `+strings.Join(conds, " OR "), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]PathEntry, 1<<10)
	for rows.Next() {
		var p string
		var e PathEntry
		if err := rows.Scan(&p, &e.MTime, &e.Size, &e.ItemId); err != nil {
			return nil, err
		}
		out[p] = e
	}
	return out, rows.Err()
}

// LoadPathIndex 一次性载入所有路径的 (mtime,size)，用于增量扫描跳过未变文件。
// 26 万条约占 30MB 内存，换来二次扫描从「全量解析」变成「只读目录」。
func (d *DB) LoadPathIndex(ctx context.Context) (map[string]PathEntry, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT path, mtime, size, item_id FROM item_paths`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]PathEntry, 1<<15)
	for rows.Next() {
		var p string
		var e PathEntry
		if err := rows.Scan(&p, &e.MTime, &e.Size, &e.ItemId); err != nil {
			return nil, err
		}
		out[p] = e
	}
	return out, rows.Err()
}

// migrateViews 给 views 表补 paths 列（多路径媒体库）。
func (d *DB) migrateViews(ctx context.Context) error {
	existing := map[string]bool{}
	rows, err := d.sql.QueryContext(ctx, `SELECT name FROM pragma_table_info('views')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		existing[n] = true
	}
	rows.Close()
	if !existing["paths"] {
		if _, err := d.sql.ExecContext(ctx, `ALTER TABLE views ADD COLUMN paths TEXT DEFAULT '[]'`); err != nil {
			return err
		}
	}
	return nil
}

// CreateAPIKey 生成并保存一个 32 位 hex 的 API Key。
func (d *DB) CreateAPIKey(ctx context.Context, note string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	key := hex.EncodeToString(b)
	_, err := d.sql.ExecContext(ctx,
		`INSERT INTO api_keys(key,note,created) VALUES(?,?,?)`, key, note, time.Now().Unix())
	if err != nil {
		return "", err
	}
	return key, nil
}

// ValidateAPIKey 判断 key 是否存在。
func (d *DB) ValidateAPIKey(ctx context.Context, key string) (bool, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT count(*) FROM api_keys WHERE key=?`, key).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ListAPIKeys 返回全部 key（用于 CLI 展示）。
func (d *DB) ListAPIKeys(ctx context.Context) ([]struct {
	Key     string
	Note    string
	Created int64
}, error) {
	rows, err := d.sql.QueryContext(ctx, `SELECT key, coalesce(note,''), created FROM api_keys ORDER BY created DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct {
		Key     string
		Note    string
		Created int64
	}
	for rows.Next() {
		var e struct {
			Key     string
			Note    string
			Created int64
		}
		if err := rows.Scan(&e.Key, &e.Note, &e.Created); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DeleteAPIKey 删除一个 key。
func (d *DB) DeleteAPIKey(ctx context.Context, key string) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM api_keys WHERE key=?`, key)
	return err
}

// SetUserItemFlag 更新收藏 / 已看状态。
func (d *DB) retryBusy(ctx context.Context, fn func() error) error {
	var err error
	for i := 0; i < 20; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "busy") {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(200*(i+1)) * time.Millisecond):
		}
	}
	return err
}

func (d *DB) SetUserItemFlag(ctx context.Context, userID, itemID string, favorite, on bool) error {
	col := "is_favorite"
	if !favorite {
		col = "played"
	}
	val := 0
	if on {
		val = 1
	}
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx,
			`INSERT INTO user_data(user_id,item_id,`+col+`) VALUES(?,?,?)
			 ON CONFLICT(user_id,item_id) DO UPDATE SET `+col+`=excluded.`+col,
			userID, itemID, val)
		return err
	})
}

// Children 按父 ID + 类型列出子条目。
func (d *DB) Children(ctx context.Context, parentID, kind string) ([]model.Item, error) {
	q := `SELECT ` + itemColumns + ` FROM items WHERE parent_id=?`
	args := []any{parentID}
	if kind != "" {
		q += " AND kind=?"
		args = append(args, kind)
	}
	q += " ORDER BY season_number, episode_number, sort_name"
	return d.queryItemsList(ctx, q, args...)
}

// SeriesEpisodes 列出剧集下的所有集（season<0 表示全部季）。
func (d *DB) SeriesEpisodes(ctx context.Context, seriesID string, season int) ([]model.Item, error) {
	q := `SELECT ` + itemColumns + ` FROM items WHERE series_id=? AND kind='Episode'`
	args := []any{seriesID}
	if season >= 0 {
		q += " AND season_number=?"
		args = append(args, season)
	}
	q += " ORDER BY season_number, episode_number"
	return d.queryItemsList(ctx, q, args...)
}

// SeasonNumber 由季条目 ID 反查季号（客户端按 SeasonId 请求集列表）。
func (d *DB) SeasonNumber(ctx context.Context, seasonID string) (int, bool) {
	var n int
	if err := d.sql.QueryRowContext(ctx, `SELECT coalesce(season_number,0) FROM items WHERE id=? AND kind='Season'`, seasonID).Scan(&n); err != nil {
		return 0, false
	}
	return n, true
}

// CountEpisodes 统计剧集下的集数（用于 ChildCount）。
func (d *DB) CountEpisodes(ctx context.Context, seriesID string) (int, error) {
	var n int
	err := d.sql.QueryRowContext(ctx,
		`SELECT count(*) FROM items WHERE series_id=? AND kind='Episode'`, seriesID).Scan(&n)
	return n, err
}

// ViewChildCount 统计媒体库下第一层可播内容数（Movie/Series），
// 供爆米花/Filmly 的 UserView 条目填充 ChildCount / RecursiveItemCount。
// 只按 parent_id 精确过滤（走 idx_items_parent_kind 索引），不做递归全表统计：
// 客户端只用这两个字段判断“目录非空”，量级不需要精确到集。
// 查询失败一律返回 0（字段照常下发，避免客户端因缺字段跳过该媒体库）。
func (d *DB) ViewChildCount(ctx context.Context, viewID string) int {
	if d == nil || viewID == "" {
		return 0
	}
	var n int
	if err := d.sql.QueryRowContext(ctx,
		`SELECT count(*) FROM items WHERE parent_id=? AND kind IN ('Movie','Series')`, viewID).Scan(&n); err != nil {
		return 0
	}
	return n
}

// imageVersionTag 生成图片版本标签：路径 + 文件大小 + 修改时间。
// 只哈路径的话，替换同名图片后标签不变，客户端会一直用旧缓存（包括占位图）。
func imageVersionTag(ref string) string {
	key := ref
	if fi, err := os.Stat(ref); err == nil {
		key = fmt.Sprintf("%s|%d|%d", ref, fi.Size(), fi.ModTime().Unix())
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}

// RenameUser 修改用户名。
func (d *DB) RenameUser(ctx context.Context, id, name string) error {
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx, `UPDATE users SET name=? WHERE id=?`, name, id)
		return err
	})
}

// SetUserAvatar 写入头像文件路径。
func (d *DB) SetUserAvatar(ctx context.Context, id, ref string) error {
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx, `UPDATE users SET avatar_ref=? WHERE id=?`, ref, id)
		return err
	})
}

// UserAvatar 读取头像路径。
func (d *DB) UserAvatar(ctx context.Context, id string) (string, error) {
	var ref string
	err := d.sql.QueryRowContext(ctx, `SELECT coalesce(avatar_ref,'') FROM users WHERE id=?`, id).Scan(&ref)
	return ref, err
}

// mergeUserDataTx 把 dup 的观看数据合并到 keeper：进度取最大、播放次数相加、
// 已看/收藏取 OR、最后播放/更新时间取最大。合并后删除 dup 的行。
func mergeUserDataTx(ctx context.Context, tx *sql.Tx, keeper, dup string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO user_data(user_id,item_id,position_ticks,duration_ticks,played,play_count,is_favorite,last_played,updated)
		SELECT user_id,?,position_ticks,duration_ticks,played,play_count,is_favorite,last_played,coalesce(updated,0)
		FROM user_data WHERE item_id=?
		ON CONFLICT(user_id,item_id) DO UPDATE SET
			position_ticks=max(user_data.position_ticks,excluded.position_ticks),
			duration_ticks=max(user_data.duration_ticks,excluded.duration_ticks),
			played=max(user_data.played,excluded.played),
			play_count=user_data.play_count+excluded.play_count,
			is_favorite=max(user_data.is_favorite,excluded.is_favorite),
			last_played=max(coalesce(user_data.last_played,0),coalesce(excluded.last_played,0)),
			updated=max(coalesce(user_data.updated,0),coalesce(excluded.updated,0))`, keeper, dup)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM user_data WHERE item_id=?`, dup)
	return err
}

// DedupeItems 合并重复条目（同一 tmdb id，或同名同年份）。
// 保留最早创建的那条，其余条目的播放源与路径归并过去后删除，返回合并掉的数量。
func (d *DB) DedupeItems(ctx context.Context) (int, error) {
	// 同时按 (kind,path) 合并：不同扫描根目录下同一文件会产生不同 ID，
	// 只按 tmdb/名称分组时这些“同路径重复”永远清不掉。
	rows, err := d.sql.QueryContext(ctx, `
		SELECT group_concat(id) FROM items WHERE kind IN ('Movie','Series')
		GROUP BY kind, coalesce(nullif(tmdb_id,''), lower(name)||'|'||year||'|'||kind)
		HAVING count(*) > 1
		UNION
		SELECT group_concat(id) FROM items WHERE kind IN ('Movie','Series') AND coalesce(path,'')<>''
		GROUP BY kind, path
		HAVING count(*) > 1`)
	if err != nil {
		return 0, err
	}
	var groups []string
	for rows.Next() {
		var ids string
		if err := rows.Scan(&ids); err != nil {
			rows.Close()
			return 0, err
		}
		groups = append(groups, ids)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	merged := 0
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	pending := 0
	for _, g := range groups {
		ids := strings.Split(g, ",")
		if len(ids) < 2 {
			continue
		}
		// 安全护栏：Movie 不应有 Season/Episode 子项。历史数据里可能存在
		// 被错误挂到 Movie 下的 Season；先跳过这类组，避免去重把子项一起搬走。
		unsafe := false
		for _, id := range ids {
			var kind string
			if err := tx.QueryRowContext(ctx, `SELECT kind FROM items WHERE id=?`, id).Scan(&kind); err != nil {
				continue
			}
			if kind != "Movie" {
				continue
			}
			var kids int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE parent_id=?`, id).Scan(&kids); err == nil && kids > 0 {
				unsafe = true
				break
			}
		}
		if unsafe {
			continue
		}
		keeper := ids[0]
		for _, dup := range ids[1:] {
			if _, err := tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=? AND source_id IN
				(SELECT source_id FROM media_sources WHERE item_id=?)`, dup, keeper); err != nil {
				tx.Rollback()
				return merged, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE media_sources SET item_id=? WHERE item_id=?`, keeper, dup); err != nil {
				tx.Rollback()
				return merged, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE item_paths SET item_id=? WHERE item_id=?`, keeper, dup); err != nil {
				tx.Rollback()
				return merged, err
			}
			// 剧集被合并时把它的季/集挂到 keeper 上，避免变成孤儿。
			if _, err := tx.ExecContext(ctx, `UPDATE items SET series_id=? WHERE series_id=?`, keeper, dup); err != nil {
				return merged, err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE items SET parent_id=? WHERE parent_id=? AND kind='Season'`, keeper, dup); err != nil {
				return merged, err
			}
			if err := mergeUserDataTx(ctx, tx, keeper, dup); err != nil {
				return merged, err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM scrape_state WHERE item_id=?`, dup); err != nil {
				return merged, err
			}
			res, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, dup)
			if err != nil {
				return merged, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				merged++
			}
			pending++
		}
		// 分块提交：单个大事务会把 WAL 撑到数 GB，并长时间阻塞检查点。
		if pending >= 200 {
			if err := tx.Commit(); err != nil {
				return merged, err
			}
			tx, err = d.sql.BeginTx(ctx, nil)
			if err != nil {
				return merged, err
			}
			pending = 0
		}
	}
	if err := tx.Commit(); err != nil {
		return merged, err
	}
	tx = nil
	// 季/集层面的重复：数据量大（线上 7000+ 组），必须用集合式 SQL 一次完成，
	// 不能按组循环（每组一次事务会把写锁占几分钟）。
	merged += d.dedupeChildren(ctx)

	// 合并后可能残留同一文件的多条来源（两种 ID 方案都写过），按路径再去一次重。
	if _, err := d.sql.ExecContext(ctx, `DELETE FROM media_sources WHERE rowid NOT IN (
		SELECT min(rowid) FROM media_sources GROUP BY item_id, path)`); err != nil {
		return merged, err
	}
	return merged, nil
}

// BackfillSeriesName 给历史单集补 series_name（旧版本 INSERT 漏了这一列）。
// 分批更新，避免一次性大事务阻塞读写。
func (d *DB) BackfillSeriesName(ctx context.Context) (int, error) {
	total := 0
	for {
		res, err := d.sql.ExecContext(ctx, `
			UPDATE items SET series_name=COALESCE((SELECT name FROM items s WHERE s.id=items.series_id), '')
			WHERE rowid IN (
				SELECT rowid FROM items
				WHERE kind='Episode' AND coalesce(series_name,'')='' AND coalesce(series_id,'')<>''
				  -- 排除悬空 series_id：否则这些行永远匹配，回填会死循环并占满写锁
				  AND EXISTS (SELECT 1 FROM items s WHERE s.id=items.series_id)
				LIMIT 1000)`)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += int(n)
		if n == 0 {
			return total, nil
		}
	}
}

// SeasonNumbersOf 返回剧集下本地已有的季号（去重、升序）。
func (d *DB) SeasonNumbersOf(ctx context.Context, seriesID string) ([]int, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT DISTINCT season_number FROM items WHERE series_id=? AND kind='Episode' ORDER BY season_number`, seriesID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// EpisodeIndex 返回 (季,集) → 条目 ID 的映射，供按 TMDB 季详情批量回填。
func (d *DB) EpisodeIndex(ctx context.Context, seriesID string) (map[[2]int]string, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, coalesce(season_number,0), coalesce(episode_number,0)
		 FROM items WHERE series_id=? AND kind='Episode'`, seriesID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[[2]int]string, 64)
	for rows.Next() {
		var id string
		var s, e int
		if err := rows.Scan(&id, &s, &e); err != nil {
			return nil, err
		}
		out[[2]int{s, e}] = id
	}
	return out, rows.Err()
}

// StillPathOf 取条目的 TMDB 缩略图路径（按主键查询）。
func (d *DB) StillPathOf(ctx context.Context, itemID string) string {
	var p string
	_ = d.sql.QueryRowContext(ctx, `SELECT coalesce(tmdb_still,'') FROM items WHERE id=?`, itemID).Scan(&p)
	return p
}

// dedupeGroups 按给定分组列合并重复条目：保留一条，播放源并入后删除其余。
func (d *DB) dedupeGroups(ctx context.Context, kindWhere, groupCols string) int {
	rows, err := d.sql.QueryContext(ctx, `SELECT group_concat(id) FROM items WHERE `+kindWhere+
		` GROUP BY `+groupCols+` HAVING count(*)>1`)
	if err != nil {
		return 0
	}
	var groups []string
	for rows.Next() {
		var ids string
		if err := rows.Scan(&ids); err == nil {
			groups = append(groups, ids)
		}
	}
	rows.Close()

	merged := 0
	for _, g := range groups {
		ids := strings.Split(g, ",")
		if len(ids) < 2 {
			continue
		}
		keeper := ids[0]
		tx, err := d.sql.BeginTx(ctx, nil)
		if err != nil {
			return merged
		}
		for _, dup := range ids[1:] {
			// 合并剧集/季之前，先把子条目指向保留者，否则会留下孤儿（列表里整部剧消失）。
			if strings.Contains(kindWhere, "Series") {
				_, _ = tx.ExecContext(ctx, `UPDATE items SET series_id=? WHERE series_id=? AND id<>?`, keeper, dup, keeper)
				_, _ = tx.ExecContext(ctx, `UPDATE items SET parent_id=? WHERE parent_id=? AND kind='Season'`, keeper, dup)
			}
			if strings.Contains(kindWhere, "Season") {
				_, _ = tx.ExecContext(ctx, `UPDATE items SET parent_id=? WHERE parent_id=? AND kind='Episode'`, keeper, dup)
			}
			_, _ = tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=? AND source_id IN
				(SELECT source_id FROM media_sources WHERE item_id=?)`, dup, keeper)
			_, _ = tx.ExecContext(ctx, `UPDATE media_sources SET item_id=? WHERE item_id=?`, keeper, dup)
			_, _ = tx.ExecContext(ctx, `UPDATE item_paths SET item_id=? WHERE item_id=?`, keeper, dup)
			// 进度取最大值合并，避免丢掉“看到一半”的记录
			_ = mergeUserDataTx(ctx, tx, keeper, dup)
			_, _ = tx.ExecContext(ctx, `DELETE FROM scrape_state WHERE item_id=?`, dup)
			_, _ = tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, dup)
			merged++
		}
		if err := tx.Commit(); err != nil {
			return merged
		}
	}
	return merged
}

// RepairOrphanSeries 把 series_id 指向已不存在剧集的季/集按剧名重新挂回（合并历史遗留）。
func (d *DB) RepairOrphanSeries(ctx context.Context) (int, error) {
	res, err := d.sql.ExecContext(ctx, `
		UPDATE items SET series_id = (
			SELECT s.id FROM items s
			WHERE s.kind='Series' AND s.name=items.series_name LIMIT 1)
		WHERE kind IN ('Season','Episode')
		  AND coalesce(series_id,'')<>''
		  AND NOT EXISTS (SELECT 1 FROM items s WHERE s.id=items.series_id)
		  AND coalesce(series_name,'')<>''
		  AND EXISTS (SELECT 1 FROM items s2 WHERE s2.kind='Series' AND s2.name=items.series_name)`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// dedupeChildren 合并重复的季/集（保留每组最小 id）。
// 关键：所有分组共用一个事务，避免每组一次 commit（7000 组会在写锁上耗几分钟）。
func (d *DB) dedupeChildren(ctx context.Context) int {
	groups := make([][2]string, 0, 8192)
	for _, spec := range []struct{ where, cols string }{
		{"kind='Episode'", "series_id, season_number, episode_number"},
		{"kind='Season'", "parent_id, season_number"},
	} {
		rows, err := d.sql.QueryContext(ctx,
			`SELECT group_concat(id ORDER BY id) FROM items WHERE `+spec.where+
				` GROUP BY `+spec.cols+` HAVING count(*)>1`)
		if err != nil {
			return 0
		}
		for rows.Next() {
			var ids string
			if err := rows.Scan(&ids); err == nil && ids != "" {
				groups = append(groups, [2]string{spec.where, ids})
			}
		}
		rows.Close()
	}
	if len(groups) == 0 {
		return 0
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	merged := 0
	pending := 0
	for _, g := range groups {
		ids := strings.Split(g[1], ",")
		if len(ids) < 2 {
			continue
		}
		keeper := ids[0]
		for _, dup := range ids[1:] {
			if strings.Contains(g[0], "Series") {
				// 兼容旧行为（当前不会走到）
				_, _ = tx.ExecContext(ctx, `UPDATE items SET series_id=? WHERE series_id=? AND id<>?`, keeper, dup, keeper)
			}
			if strings.Contains(g[0], "Season") {
				_, _ = tx.ExecContext(ctx, `UPDATE items SET parent_id=? WHERE parent_id=? AND kind='Episode'`, keeper, dup)
			}
			_, _ = tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=? AND source_id IN
				(SELECT source_id FROM media_sources WHERE item_id=?)`, dup, keeper)
			_, _ = tx.ExecContext(ctx, `UPDATE media_sources SET item_id=? WHERE item_id=?`, keeper, dup)
			_, _ = tx.ExecContext(ctx, `UPDATE OR IGNORE item_paths SET item_id=? WHERE item_id=?`, keeper, dup)
			_, _ = tx.ExecContext(ctx, `DELETE FROM item_paths WHERE item_id=?`, dup)
			_ = mergeUserDataTx(ctx, tx, keeper, dup)
			_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO scrape_state(item_id,status,source,error,attempts,updated)
				SELECT ?,status,source,error,attempts,updated FROM scrape_state WHERE item_id=?`, keeper, dup)
			_, _ = tx.ExecContext(ctx, `DELETE FROM scrape_state WHERE item_id=?`, dup)
			_, _ = tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, dup)
			merged++
			pending++
		}
		if pending >= 200 {
			if err := tx.Commit(); err != nil {
				return merged
			}
			tx, err = d.sql.BeginTx(ctx, nil)
			if err != nil {
				return merged
			}
			pending = 0
		}
	}
	if err := tx.Commit(); err != nil {
		return merged
	}
	tx = nil
	return merged
}

// NormalizeLibraryParents 把顶层条目（电影/剧集）的 parent_id 归位到类型匹配的媒体库。
// 背景：两个库都挂在 /test 时，扫描会把所有条目挂到“先扫的那个库”，
// 客户端（VidHub）按 parent 归组，于是剧集出现在电影库里。
func (d *DB) NormalizeLibraryParents(ctx context.Context) (int, error) {
	views, err := d.GetViews(ctx)
	if err != nil {
		return 0, err
	}
	type rule struct {
		prefix string
		kind   string
		libID  string
	}
	var rules []rule
	for _, v := range views {
		kind := ""
		switch v.CollectionType {
		case "movies":
			kind = "Movie"
		case "tvshows":
			kind = "Series"
		default:
			continue
		}
		for _, p := range v.Paths {
			p = strings.TrimRight(strings.TrimSpace(p), "/")
			if p != "" {
				rules = append(rules, rule{prefix: p, kind: kind, libID: v.Id})
			}
		}
	}
	if len(rules) == 0 {
		return 0, nil
	}
	// 最长前缀优先：/a/b 比 /a 更具体。
	sort.Slice(rules, func(i, j int) bool { return len(rules[i].prefix) > len(rules[j].prefix) })

	rows, err := d.sql.QueryContext(ctx,
		`SELECT id, kind, coalesce(path,''), coalesce(parent_id,'') FROM items WHERE kind IN ('Movie','Series')`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	updates := map[string][]string{}
	var orphans []string
	for rows.Next() {
		var id, kind, p, parent string
		if err := rows.Scan(&id, &kind, &p, &parent); err != nil {
			return 0, err
		}
		want := ""
		for _, r := range rules {
			if r.kind != kind {
				continue
			}
			if p == r.prefix || strings.HasPrefix(p, r.prefix+"/") {
				want = r.libID
				break
			}
		}
		if want != "" {
			if want != parent {
				updates[want] = append(updates[want], id)
			}
		} else if parent != "" {
			// 不属于任何已添加媒体库：清空归属，避免出现在影片列表/搜索里。
			orphans = append(orphans, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(updates) == 0 && len(orphans) == 0 {
		return 0, nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	total := 0
	const chunk = 500
	for start := 0; start < len(orphans); start += chunk {
		end := start + chunk
		if end > len(orphans) {
			end = len(orphans)
		}
		ph := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		args := make([]any, 0, end-start)
		for _, id := range orphans[start:end] {
			args = append(args, id)
		}
		res, err := tx.ExecContext(ctx, `UPDATE items SET parent_id='' WHERE id IN (`+ph+`)`, args...)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += int(n)
	}
	for libID, ids := range updates {
		for start := 0; start < len(ids); start += chunk {
			end := start + chunk
			if end > len(ids) {
				end = len(ids)
			}
			ph := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
			args := make([]any, 0, end-start+1)
			args = append(args, libID)
			for _, id := range ids[start:end] {
				args = append(args, id)
			}
			res, err := tx.ExecContext(ctx, `UPDATE items SET parent_id=? WHERE id IN (`+ph+`)`, args...)
			if err != nil {
				return total, err
			}
			n, _ := res.RowsAffected()
			total += int(n)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}

// ItemPath 返回条目路径（主键查询）。
func (d *DB) ItemPath(ctx context.Context, id string) string {
	var p string
	_ = d.sql.QueryRowContext(ctx, `SELECT coalesce(path,'') FROM items WHERE id=?`, id).Scan(&p)
	return p
}

// SetMediaSourceSize 缓存某个播放源的字节大小。
func (d *DB) SetMediaSourceSize(ctx context.Context, itemID, sourceID string, size int64) error {
	_, err := d.sql.ExecContext(ctx, `UPDATE media_sources SET size=? WHERE item_id=? AND source_id=?`, size, itemID, sourceID)
	return err
}

// MediaProbe 是上游媒体的一次探测结果（不保存视频内容）。
type MediaProbe struct {
	ItemID       string
	SourceID     string
	Target       string
	Size         int64
	ETag         string
	LastModified string
	Tracks       string // JSON 数组，字段由 httpapi 解析
	Error        string
	ProbedAt     int64
	CheckedAt    int64
}

// GetMediaProbe 读取某个播放源的探测缓存。
func (d *DB) GetMediaProbe(ctx context.Context, itemID, sourceID string) (*MediaProbe, error) {
	var p MediaProbe
	err := d.sql.QueryRowContext(ctx,
		`SELECT target, coalesce(size,0), coalesce(etag,''), coalesce(last_modified,''), coalesce(tracks,'[]'), coalesce(error,''), coalesce(probed_at,0), coalesce(checked_at,0)
		 FROM media_probes WHERE item_id=? AND source_id=?`, itemID, sourceID).
		Scan(&p.Target, &p.Size, &p.ETag, &p.LastModified, &p.Tracks, &p.Error, &p.ProbedAt, &p.CheckedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.ItemID, p.SourceID = itemID, sourceID
	return &p, nil
}

// SaveMediaProbe 保存探测结果，并同步 media_sources.size 供列表/客户端直接读取。
func (d *DB) SaveMediaProbe(ctx context.Context, p MediaProbe) error {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO media_probes(item_id,source_id,target,size,etag,last_modified,tracks,error,probed_at,checked_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(item_id,source_id) DO UPDATE SET
		   target=excluded.target, size=excluded.size, etag=excluded.etag,
		   last_modified=excluded.last_modified, tracks=excluded.tracks,
		   error=excluded.error, probed_at=excluded.probed_at, checked_at=excluded.checked_at`,
		p.ItemID, p.SourceID, p.Target, p.Size, p.ETag, p.LastModified, p.Tracks, p.Error, p.ProbedAt, p.CheckedAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE media_sources SET size=? WHERE item_id=? AND source_id=?`, p.Size, p.ItemID, p.SourceID); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkMediaProbeChecked 记录一次 ETag/大小校验时间。
func (d *DB) MarkMediaProbeChecked(ctx context.Context, itemID, sourceID string, checkedAt int64) error {
	_, err := d.sql.ExecContext(ctx,
		`UPDATE media_probes SET checked_at=? WHERE item_id=? AND source_id=?`, checkedAt, itemID, sourceID)
	return err
}

// DeleteMediaProbe 删除某个播放源的探测缓存。
func (d *DB) DeleteMediaProbe(ctx context.Context, itemID, sourceID string) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM media_probes WHERE item_id=? AND source_id=?`, itemID, sourceID)
	return err
}

// migrateV16 缓存上游媒体探测结果；新表由 schema.sql 创建，老库在这里补建。
func (d *DB) migrateV16(ctx context.Context) error {
	_, err := d.sql.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS media_probes (
		  item_id TEXT NOT NULL, source_id TEXT NOT NULL, target TEXT NOT NULL DEFAULT '',
		  size INTEGER NOT NULL DEFAULT 0, etag TEXT NOT NULL DEFAULT '',
		  last_modified TEXT NOT NULL DEFAULT '', tracks TEXT NOT NULL DEFAULT '[]',
		  error TEXT NOT NULL DEFAULT '', probed_at INTEGER NOT NULL DEFAULT 0, checked_at INTEGER NOT NULL DEFAULT 0,
		  PRIMARY KEY (item_id, source_id)
		);
		CREATE INDEX IF NOT EXISTS idx_media_probes_item ON media_probes(item_id);`)
	return err
}

// GetSetting / SetSetting：通用键值设置（定时扫描计划等）。
func (d *DB) GetSetting(ctx context.Context, key string) string {
	var v string
	_ = d.sql.QueryRowContext(ctx, `SELECT v FROM settings WHERE k=?`, key).Scan(&v)
	return v
}

// HasSetting 判断某个设置是否明确保存过（用于区分“空值”和“未设置”）。
func (d *DB) HasSetting(ctx context.Context, key string) bool {
	var n int
	return d.sql.QueryRowContext(ctx, `SELECT count(*) FROM settings WHERE k=?`, key).Scan(&n) == nil && n > 0
}

func (d *DB) SetSetting(ctx context.Context, key, value string) error {
	_, err := d.sql.ExecContext(ctx, `INSERT INTO settings(k,v) VALUES(?,?)
		ON CONFLICT(k) DO UPDATE SET v=excluded.v`, key, value)
	return err
}

// SeriesMeta 取剧集的名称与 TMDB id（缺失分集统计用，避免加载完整条目）。
func (d *DB) SeriesMeta(ctx context.Context, id string) (name string, tmdbID int64, ok bool) {
	var provJSON string
	var kind string
	if err := d.sql.QueryRowContext(ctx,
		`SELECT kind, name, coalesce(provider_ids,'{}') FROM items WHERE id=?`, id).Scan(&kind, &name, &provJSON); err != nil {
		return "", 0, false
	}
	if kind != "Series" {
		return "", 0, false
	}
	var prov map[string]string
	_ = json.Unmarshal([]byte(provJSON), &prov)
	if v, err := strconv.ParseInt(prov["tmdb"], 10, 64); err == nil {
		tmdbID = v
	}
	return name, tmdbID, true
}

// ListSeriesIDs 返回有 TMDB id 的剧集（分页），用于无 ParentId 的缺失统计。
func (d *DB) ListSeriesIDs(ctx context.Context, limit, offset int) ([]string, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id FROM items WHERE kind='Series' AND provider_ids LIKE '%"tmdb"%'
		 ORDER BY sort_name, id LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// MissingCoverIDs 返回缺少封面（image_ref 为 NULL/空）的条目 id，按 id 排序，最多 limit 条。
// 供 `-backfill-covers` CLI 逐条重新刮削补图（不在服务启动时自动跑）；
// kinds 为空时按 Movie+Series 算，limit<=0 取 100。
func (d *DB) MissingCoverIDs(ctx context.Context, kinds []string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.sql.QueryContext(ctx,
		`SELECT id FROM items WHERE kind IN `+kindFilter(kinds)+`
		 AND (image_ref IS NULL OR image_ref='') ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ResetSearchIndex 让内存搜索索引下次请求时重建（删除库/条目后调用）。
func (d *DB) ResetSearchIndex() {
	markSearchDirty()
	d.si.mu.Lock()
	d.si.loaded = false
	d.si.entries = nil
	d.si.mu.Unlock()
}

// RemoveLibrary 只移除媒体库及其数据库条目，不触碰磁盘上的任何媒体文件。
func (d *DB) RemoveLibrary(ctx context.Context, id string) (int, error) {
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM views WHERE id=?`, id); err != nil {
		return 0, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id FROM items WHERE parent_id=? AND kind IN ('Movie','Series')`, id)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var iid string
		if err := rows.Scan(&iid); err == nil {
			ids = append(ids, iid)
		}
	}
	rows.Close()

	// 剧集/电影的子条目（季、集）
	if len(ids) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		args := make([]any, len(ids))
		for i, v := range ids {
			args[i] = v
		}
		rows2, err := tx.QueryContext(ctx,
			`SELECT id FROM items WHERE parent_id IN (`+ph+`) OR series_id IN (`+ph+`)`, append(args, args...)...)
		if err == nil {
			for rows2.Next() {
				var iid string
				if err := rows2.Scan(&iid); err == nil {
					ids = append(ids, iid)
				}
			}
			rows2.Close()
		}
	}

	deleted := 0
	for _, iid := range ids {
		for _, q := range []string{
			`DELETE FROM media_sources WHERE item_id=?`,
			`DELETE FROM user_data WHERE item_id=?`,
			`DELETE FROM scrape_state WHERE item_id=?`,
			`DELETE FROM item_paths WHERE item_id=?`,
			`DELETE FROM items WHERE id=?`,
		} {
			if _, err := tx.ExecContext(ctx, q, iid); err != nil {
				return deleted, err
			}
		}
		deleted++
	}
	if err := tx.Commit(); err != nil {
		return deleted, err
	}
	d.ResetSearchIndex()
	return deleted, nil
}

// OptimizeStats 重建 SQLite 统计信息。默认 PRAGMA optimize 的采样不足以
// 修正大表上退化的计划，这里用 analysis_limit=0 做完整 ANALYZE。
func (d *DB) OptimizeStats(ctx context.Context) error {
	if _, err := d.sql.ExecContext(ctx, `PRAGMA analysis_limit=0`); err != nil {
		return err
	}
	_, err := d.sql.ExecContext(ctx, `ANALYZE`)
	return err
}

// streamsForSource 从版本标签/文件名推断视频流属性。
// 扫描阶段不探测真实容器（.strm 指向网盘），但版本标签已经带有
// 4K/1080P、HDR/Dovi、杜比视界等信息，客户端（Moonfin 等）靠这些
// 判断同一媒体的多个版本并展示切换入口。
//
// 同时给出一条默认音轨（Audio, Index=1）：Emby 客户端会用 MediaStreams 里的音轨
// 决定能否初始化播放器，只有 Video 一条时部分客户端（Filmly）会直接判定“无法播放”。
func streamsForSource(name, path string) []model.MediaStream {
	hay := strings.ToLower(name + " " + path)
	vs := model.MediaStream{Index: 0, Type: "Video", IsDefault: true}
	// 版本标签放进标准流标题，避免 VidHub 电视版回退到文件名。
	if strings.TrimSpace(name) != "" {
		vs.Title = name
		vs.DisplayTitle = name
	}
	switch {
	case strings.Contains(hay, "h265") || strings.Contains(hay, "hevc") || strings.Contains(hay, "265"):
		vs.Codec = "hevc"
	default:
		vs.Codec = "h264"
	}
	switch {
	case strings.Contains(hay, "杜比视界") || strings.Contains(hay, "dovi") || strings.Contains(hay, "dolby vision"):
		vs.VideoRange, vs.VideoRangeType = "HDR", "DOVI"
	case strings.Contains(hay, "hdr10+") || strings.Contains(hay, "hdr10plus"):
		vs.VideoRange, vs.VideoRangeType = "HDR", "HDR10Plus"
	case strings.Contains(hay, "hdr10"):
		vs.VideoRange, vs.VideoRangeType = "HDR", "HDR10"
	case strings.Contains(hay, "hdr"):
		vs.VideoRange, vs.VideoRangeType = "HDR", "HDR10"
	case strings.Contains(hay, "hlg"):
		vs.VideoRange, vs.VideoRangeType = "HDR", "HLG"
	default:
		vs.VideoRange, vs.VideoRangeType = "SDR", "SDR"
	}
	if vs.VideoRangeType != "SDR" {
		vs.ColorTransfer, vs.ColorPrimaries, vs.BitDepth = "smpte2084", "bt2020", 10
	} else {
		vs.ColorTransfer, vs.ColorPrimaries, vs.BitDepth = "bt709", "bt709", 8
	}
	switch {
	case strings.Contains(hay, "4k") || strings.Contains(hay, "2160"):
		vs.Width, vs.Height = 3840, 2160
	case strings.Contains(hay, "1080"):
		vs.Width, vs.Height = 1920, 1080
	case strings.Contains(hay, "720"):
		vs.Width, vs.Height = 1280, 720
	}
	if strings.Contains(hay, "10bit") {
		vs.BitDepth = 10
	}

	as := model.MediaStream{Index: 1, Type: "Audio", Language: "chi", IsDefault: true}
	switch {
	case strings.Contains(hay, "truehd"):
		as.Codec, as.Channels = "truehd", 8
	case strings.Contains(hay, "atmos") || strings.Contains(hay, "ddp") || strings.Contains(hay, "eac3") || strings.Contains(hay, "dd+"):
		as.Codec, as.Channels = "eac3", 6
	case strings.Contains(hay, "dts"):
		as.Codec, as.Channels = "dts", 6
	case strings.Contains(hay, "ac3"):
		as.Codec, as.Channels = "ac3", 6
	case strings.Contains(hay, "flac"):
		as.Codec, as.Channels = "flac", 2
	default:
		as.Codec, as.Channels = "aac", 2
	}
	return []model.MediaStream{vs, as}
}
