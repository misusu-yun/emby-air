package store

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// TickPerSecond 是 Emby 的时间单位：1 tick = 100ns。
const TickPerSecond = 10_000_000

// PlaybackProgress 是某个用户在某条媒体上的最新进度。
type PlaybackProgress struct {
	UserID        string
	ItemID        string
	PositionTicks int64
	DurationTicks int64
	Played        bool
	// PlayInc 为 1 表示这次上报要累加播放次数（只应在 Stopped 时置 1）。
	PlayInc    int
	LastPlayed int64 // unix 秒
	Updated    int64 // unix 毫秒，用于丢弃乱序到达的旧进度
}

// UpsertPlaybacks 批量写入进度：同一 (user,item) 只保留最后一条，且旧数据不覆盖新数据。
func (d *DB) UpsertPlaybacks(ctx context.Context, list []PlaybackProgress) error {
	if len(list) == 0 {
		return nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO user_data(user_id,item_id,position_ticks,duration_ticks,played,play_count,last_played,updated)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(user_id,item_id) DO UPDATE SET
			position_ticks=excluded.position_ticks,
			duration_ticks=excluded.duration_ticks,
			played=excluded.played,
			play_count=user_data.play_count+excluded.play_count,
			last_played=excluded.last_played,
			updated=excluded.updated
		WHERE excluded.updated >= user_data.updated`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range list {
		played := 0
		if p.Played {
			played = 1
		}
		if _, err := stmt.ExecContext(ctx, p.UserID, p.ItemID, p.PositionTicks, p.DurationTicks,
			played, p.PlayInc, p.LastPlayed, p.Updated); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetPlayback 读取某用户在某条目上的已存进度；不存在时 ok=false。
func (d *DB) GetPlayback(ctx context.Context, userID, itemID string) (PlaybackProgress, bool, error) {
	var p PlaybackProgress
	var played int
	err := d.sql.QueryRowContext(ctx, `
		SELECT position_ticks, duration_ticks, played, coalesce(last_played,0), coalesce(updated,0)
		FROM user_data WHERE user_id=? AND item_id=?`, userID, itemID).
		Scan(&p.PositionTicks, &p.DurationTicks, &played, &p.LastPlayed, &p.Updated)
	if err == sql.ErrNoRows {
		return p, false, nil
	}
	if err != nil {
		return p, false, err
	}
	p.UserID, p.ItemID, p.Played = userID, itemID, played == 1
	return p, true, nil
}

// SessionRow 是一条实时播放会话（不是登录会话）。
type SessionRow struct {
	ID            string
	UserID        string
	UserName      string
	DeviceID      string
	Client        string
	ClientVersion string
	DeviceName    string
	ItemID        string
	ItemName      string
	PositionTicks int64
	RuntimeTicks  int64
	IsPaused      bool
	PlayMethod    string
	IP            string
	Started       int64
	Updated       int64
}

// UpsertSession 新建或更新播放会话；切到别的条目时重置 started。
func (d *DB) UpsertSession(ctx context.Context, s SessionRow) error {
	paused := 0
	if s.IsPaused {
		paused = 1
	}
	return d.retryBusy(ctx, func() error {
		_, err := d.sql.ExecContext(ctx, `
			INSERT INTO sessions(id,user_id,user_name,device_id,client,client_version,device_name,
				item_id,item_name,position_ticks,runtime_ticks,is_paused,play_method,ip,started,updated)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(id) DO UPDATE SET
				user_id=excluded.user_id, user_name=excluded.user_name,
				device_id=excluded.device_id, client=excluded.client,
				client_version=excluded.client_version, device_name=excluded.device_name,
				item_id=excluded.item_id, item_name=excluded.item_name,
				position_ticks=excluded.position_ticks, runtime_ticks=excluded.runtime_ticks,
				is_paused=excluded.is_paused, play_method=excluded.play_method,
				ip=excluded.ip, updated=excluded.updated,
				started=CASE WHEN sessions.item_id<>excluded.item_id THEN excluded.started ELSE sessions.started END`,
			s.ID, s.UserID, s.UserName, s.DeviceID, s.Client, s.ClientVersion, s.DeviceName,
			s.ItemID, s.ItemName, s.PositionTicks, s.RuntimeTicks, paused, s.PlayMethod, s.IP,
			s.Started, s.Updated)
		return err
	})
}

// ActiveSessions 返回 updated >= since 的会话，按开始时间倒序。
func (d *DB) ActiveSessions(ctx context.Context, since int64) ([]SessionRow, error) {
	rows, err := d.sql.QueryContext(ctx, `
		SELECT id,user_id,user_name,device_id,client,client_version,device_name,
		       item_id,item_name,position_ticks,runtime_ticks,is_paused,play_method,ip,started,updated
		FROM sessions WHERE updated>=? ORDER BY started DESC`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionRow
	for rows.Next() {
		var s SessionRow
		var paused int
		if err := rows.Scan(&s.ID, &s.UserID, &s.UserName, &s.DeviceID, &s.Client, &s.ClientVersion,
			&s.DeviceName, &s.ItemID, &s.ItemName, &s.PositionTicks, &s.RuntimeTicks, &paused,
			&s.PlayMethod, &s.IP, &s.Started, &s.Updated); err != nil {
			return nil, err
		}
		s.IsPaused = paused == 1
		out = append(out, s)
	}
	return out, rows.Err()
}

// CloseSession 结束一条播放会话（客户端发 Stopped 时调用）。
func (d *DB) CloseSession(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := d.sql.ExecContext(ctx, `DELETE FROM sessions WHERE id=?`, id)
	return err
}

// PruneSessions 清理心跳超时的僵尸会话。
func (d *DB) PruneSessions(ctx context.Context, before int64) error {
	_, err := d.sql.ExecContext(ctx, `DELETE FROM sessions WHERE updated<?`, before)
	return err
}

// Activity 是一条活动记录。
type Activity struct {
	ID       int64  `json:"Id"`
	Ts       int64  `json:"Date"`
	UserID   string `json:"UserId"`
	UserName string `json:"UserName"`
	Kind     string `json:"Kind"`
	ItemID   string `json:"ItemId,omitempty"`
	ItemName string `json:"ItemName,omitempty"`
	Client   string `json:"Client,omitempty"`
	Device   string `json:"Device,omitempty"`
	IP       string `json:"Ip,omitempty"`
	Detail   string `json:"Detail,omitempty"`
}

// AddActivity 写入活动并裁剪到最近 5000 条。
func (d *DB) AddActivity(ctx context.Context, a Activity) error {
	if a.Ts == 0 {
		a.Ts = time.Now().UnixMilli()
	}
	return d.retryBusy(ctx, func() error {
		if _, err := d.sql.ExecContext(ctx, `
			INSERT INTO activities(ts,user_id,user_name,kind,item_id,item_name,client,device,ip,detail)
			VALUES(?,?,?,?,?,?,?,?,?,?)`,
			a.Ts, a.UserID, a.UserName, a.Kind, a.ItemID, a.ItemName, a.Client, a.Device, a.IP, a.Detail); err != nil {
			return err
		}
		_, err := d.sql.ExecContext(ctx, `
			DELETE FROM activities WHERE id <= (SELECT max(id)-5000 FROM activities)`)
		return err
	})
}

// Activities 按时间倒序分页；before>0 时只取更早的记录。
func (d *DB) Activities(ctx context.Context, limit int, before int64) ([]Activity, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id,ts,user_id,user_name,kind,item_id,item_name,client,device,ip,detail FROM activities`
	args := []any{}
	if before > 0 {
		q += ` WHERE ts < ?`
		args = append(args, before)
	}
	q += ` ORDER BY ts DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := d.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Activity
	for rows.Next() {
		var a Activity
		if err := rows.Scan(&a.ID, &a.Ts, &a.UserID, &a.UserName, &a.Kind, &a.ItemID, &a.ItemName,
			&a.Client, &a.Device, &a.IP, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeviceRow 是设备指纹记录。
type DeviceRow struct {
	DeviceID   string
	UserID     string
	Client     string
	DeviceName string
	Version    string
	Kind       string
	IP         string
}

// TouchDevice 记录/更新一台设备。
func (d *DB) TouchDevice(ctx context.Context, dev DeviceRow) error {
	now := time.Now().Unix()
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO devices(device_id,user_id,client,device_name,version,kind,ip,first_seen,last_seen)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(device_id) DO UPDATE SET
			user_id=excluded.user_id, client=excluded.client, device_name=excluded.device_name,
			version=excluded.version, kind=excluded.kind, ip=excluded.ip, last_seen=excluded.last_seen`,
		dev.DeviceID, dev.UserID, dev.Client, dev.DeviceName, dev.Version, dev.Kind, dev.IP, now, now)
	return err
}

// CountMediaSourcePath 返回某个文件被多少条来源引用（多版本可能共用文件）。
func (d *DB) CountMediaSourcePath(ctx context.Context, path string) (int, error) {
	var n int
	err := d.sql.QueryRowContext(ctx, `SELECT count(*) FROM media_sources WHERE path=?`, path).Scan(&n)
	return n, err
}

// LocalSource 是数据库里的原始来源记录（Path 是本地 .strm 路径，不是解析后的直链）。
type LocalSource struct {
	ID   string
	Path string
}

// LocalSources 返回条目在库里的原始来源（按 sort_order）。
func (d *DB) LocalSources(ctx context.Context, itemID string) ([]LocalSource, error) {
	rows, err := d.sql.QueryContext(ctx,
		`SELECT source_id, path FROM media_sources WHERE item_id=? ORDER BY sort_order, source_id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LocalSource
	for rows.Next() {
		var s LocalSource
		if err := rows.Scan(&s.ID, &s.Path); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// RemoveMediaSources 删除条目的指定播放源（按 source_id）；条目没有剩余来源时一并删除，
// 并向上清理空的 Season / Series。
func (d *DB) RemoveMediaSources(ctx context.Context, itemID string, sourceIDs []string) error {
	if len(sourceIDs) == 0 {
		return nil
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, sid := range sourceIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=? AND source_id=?`, itemID, sid); err != nil {
			return err
		}
	}

	var left int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM media_sources WHERE item_id=?`, itemID).Scan(&left); err != nil {
		return err
	}
	if left == 0 {
		var kind, parentID string
		if err := tx.QueryRowContext(ctx, `SELECT kind, coalesce(parent_id,'') FROM items WHERE id=?`, itemID).Scan(&kind, &parentID); err != nil && err != sql.ErrNoRows {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, itemID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM media_sources WHERE item_id=?`, itemID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM user_data WHERE item_id=?`, itemID); err != nil {
			return err
		}
		// 季/剧集：没有任何子项时删除。
		if kind == "Episode" && parentID != "" {
			var kids int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE parent_id=?`, parentID).Scan(&kids); err == nil && kids == 0 {
				var seasonParent string
				_ = tx.QueryRowContext(ctx, `SELECT coalesce(parent_id,'') FROM items WHERE id=?`, parentID).Scan(&seasonParent)
				if _, err := tx.ExecContext(ctx, `DELETE FROM items WHERE id=?`, parentID); err == nil && seasonParent != "" {
					var seasons int
					if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM items WHERE parent_id=?`, seasonParent).Scan(&seasons); err == nil && seasons == 0 {
						_, _ = tx.ExecContext(ctx, `DELETE FROM items WHERE id=? AND kind='Series'`, seasonParent)
					}
				}
			}
		}
	}
	return tx.Commit()
}

// normalizePath 只用于比较，不改动实际路径。
func normalizePath(p string) string { return strings.TrimRight(p, "/") }

// ItemContext 是会话展示需要的剧集上下文（避免客户端只看到“第 N 集”）。
type ItemContext struct {
	Kind          string
	SeriesName    string
	SeriesID      string
	SeasonNumber  int
	EpisodeNumber int
}

// ItemContextOf 查单集/条目的上级剧集信息（按 id 主键查询，单次 ~0.1ms）。
func (d *DB) ItemContextOf(ctx context.Context, itemID string) (ItemContext, error) {
	var c ItemContext
	err := d.sql.QueryRowContext(ctx, `
		SELECT kind, coalesce(series_name,''), coalesce(series_id,''),
		       coalesce(season_number,0), coalesce(episode_number,0)
		FROM items WHERE id=?`, itemID).
		Scan(&c.Kind, &c.SeriesName, &c.SeriesID, &c.SeasonNumber, &c.EpisodeNumber)
	return c, err
}
