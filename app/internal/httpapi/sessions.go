package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"emby-air/internal/store"
)

// sessionIdle 是会话心跳超时：客户端每 10 秒上报一次，60 秒没动静视为掉线。
const sessionIdle = 60 * time.Second

// progressAgg 聚合进度写入：内存保留每个 (user,item) 的最新一条，定时批量落库。
// SQLite 是单写者，直接按上报频率写会放大锁竞争。
type progressAgg struct {
	mu   sync.Mutex
	buf  map[[2]string]store.PlaybackProgress
	db   *store.DB
	stop chan struct{}
	once sync.Once
	// wg 等待两个后台循环退出，Close 返回后不再访问 DB。
	wg sync.WaitGroup
}

func newProgressAgg(db *store.DB) *progressAgg {
	return &progressAgg{buf: make(map[[2]string]store.PlaybackProgress), db: db, stop: make(chan struct{})}
}

func (a *progressAgg) Touch(p store.PlaybackProgress) {
	a.mu.Lock()
	a.buf[[2]string{p.UserID, p.ItemID}] = p
	a.mu.Unlock()
}

// Peek 返回内存中尚未落库的进度（可能是比库里更新的值）。
func (a *progressAgg) Peek(userID, itemID string) (store.PlaybackProgress, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.buf[[2]string{userID, itemID}]
	return p, ok
}

func (a *progressAgg) Flush(ctx context.Context) {
	a.mu.Lock()
	batch := a.buf
	a.buf = make(map[[2]string]store.PlaybackProgress, len(batch))
	a.mu.Unlock()
	if len(batch) == 0 || a.db == nil {
		return
	}
	list := make([]store.PlaybackProgress, 0, len(batch))
	for _, p := range batch {
		list = append(list, p)
	}
	// 落库失败也不重试：下一次上报会带来更新数据。
	_ = a.db.UpsertPlaybacks(ctx, list)
}

// start 在后台每 5 秒 flush 一次，并清理僵尸会话。
func (a *progressAgg) start() {
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-tick.C:
				a.Flush(context.Background())
			}
		}
	}()
	go func() {
		defer a.wg.Done()
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-a.stop:
				return
			case <-tick.C:
				if a.db != nil {
					_ = a.db.PruneSessions(context.Background(), time.Now().Add(-sessionIdle).Unix())
				}
			}
		}
	}()
}

func (a *progressAgg) close() {
	a.once.Do(func() { close(a.stop) })
	a.wg.Wait()
	a.Flush(context.Background())
}

// sessionGuard 记录每个播放会话的“基线位置”。
// VidHub 起播后会先发一条 ~1 秒的 Progress（此时还没 seek 到续播点），
// 如果不拦住，这条会把已保存的 165 秒进度冲掉——这是续播归零的直接原因。
// 每个会话只拦截“第一条且明显低于基线”的 Progress；用户从头播放时，
// 第二条 Progress（已经 10 秒+）会正常写回，不会把用户卡死在旧进度。
type sessionGuard struct {
	mu  sync.Mutex
	m   map[string]*sessionGuardEntry
	cap int
}

type sessionGuardEntry struct {
	baseline    int64
	seenFirst   bool
	lastTouched int64
}

func (g *sessionGuard) entry(id string, baseline int64) *sessionGuardEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m == nil {
		g.m = make(map[string]*sessionGuardEntry)
		g.cap = 256
	}
	// 简单容量控制：超限时清掉最久未更新的，避免内存无界增长。
	if len(g.m) >= g.cap {
		var oldestKey string
		var oldest int64 = 1<<62 - 1
		for k, v := range g.m {
			if v.lastTouched < oldest {
				oldest, oldestKey = v.lastTouched, k
			}
		}
		delete(g.m, oldestKey)
	}
	e, ok := g.m[id]
	if !ok {
		e = &sessionGuardEntry{baseline: baseline, lastTouched: time.Now().Unix()}
		g.m[id] = e
	}
	e.lastTouched = time.Now().Unix()
	return e
}

func (g *sessionGuard) drop(id string) {
	g.mu.Lock()
	delete(g.m, id)
	g.mu.Unlock()
}

// playbackTrace 是最近播放上报的有界环形记录（仅用于排查客户端行为差异）。
type playbackTrace struct {
	mu  sync.Mutex
	buf []map[string]any
	max int
}

func (t *playbackTrace) add(rec map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.max == 0 {
		t.max = 50
	}
	if len(t.buf) >= t.max {
		copy(t.buf, t.buf[1:])
		t.buf = t.buf[:len(t.buf)-1]
	}
	t.buf = append(t.buf, rec)
}

func (t *playbackTrace) snapshot() []map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]map[string]any, len(t.buf))
	copy(out, t.buf)
	return out
}

// handleSessionPing 处理 Sessions/Playing、Playing/Progress、Playing/Stopped。
// 未知字段一律忽略，保证不同客户端（VidHub/Emby/Infuse）都能过。
func (s *Server) handleSessionPing(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	// 进度上报永远返回 204：任何解析失败都不该打断客户端播放。
	w.WriteHeader(http.StatusNoContent)
	if s.db == nil {
		return
	}

	var body struct {
		ItemId        string `json:"ItemId"`
		PlaySessionId string `json:"PlaySessionId"`
		PositionTicks int64  `json:"PositionTicks"`
		IsPaused      bool   `json:"IsPaused"`
		PlayMethod    string `json:"PlayMethod"`
	}
	if err := decodeJSONBody(r, &body); err != nil || body.ItemId == "" {
		return
	}
	it, err := s.db.GetItem(r.Context(), body.ItemId)
	if err != nil || it == nil {
		return
	}

	path := strings.TrimSuffix(r.URL.Path, "/")
	stopped := strings.HasSuffix(path, "/Stopped")
	// /Sessions/Playing 是“开始播放”事件，不是进度上报。
	started := strings.HasSuffix(path, "/Playing")
	now := time.Now()

	// 续播被打回 0 的根因（VidHub 实测时序）：
	//   GET item detail → POST PlaybackInfo → POST Sessions/Playing{PositionTicks:0}
	// 开始事件里的 0 是默认值而非真实进度，落库会把已存的 30 秒抹掉。
	pos := body.PositionTicks

	if stopped {
		// 收尾取“已知最大值”：内存聚合器可能比库新，库里可能有更早的真实进度。
		if p, ok := s.progress.Peek(u.Id, it.Id); ok && p.PositionTicks > pos {
			pos = p.PositionTicks
		}
		if prev, ok, err := s.db.GetPlayback(r.Context(), u.Id, it.Id); err == nil && ok && prev.PositionTicks > pos {
			pos = prev.PositionTicks
		}
	}
	// Progress 是播放中的连续值；只拦截“会话第一条且明显低于基线”的异常上报，
	// 其余（包括真实拖动回退）照常写入。
	guardID := body.PlaySessionId
	if guardID == "" {
		guardID = u.Id + ":" + it.Id
	}
	if !stopped && !started {
		stored := int64(0)
		if prev, ok, err := s.db.GetPlayback(r.Context(), u.Id, it.Id); err == nil && ok {
			stored = prev.PositionTicks
		}
		if p, ok := s.progress.Peek(u.Id, it.Id); ok && p.PositionTicks > stored {
			stored = p.PositionTicks
		}
		e := s.guard.entry(guardID, stored)
		if !e.seenFirst {
			e.seenFirst = true
			if e.baseline-pos > 30*store.TickPerSecond {
				pos = e.baseline // VidHub 起播瞬间的“假 0”，跳过不写
			}
		}
	} else {
		s.guard.drop(guardID)
	}
	// 只有 Progress 与 Stopped 写进度；开始事件只建会话。
	writeProgress := !started && (pos > 0 || stopped)

	if writeProgress {
		pct := 0.0
		if it.RunTimeTicks > 0 {
			pct = float64(pos) / float64(it.RunTimeTicks)
		}
		played := it.RunTimeTicks > 0 && (pct >= 0.90 || it.RunTimeTicks-pos < 60*store.TickPerSecond)
		playInc := 0
		if stopped {
			playInc = 1
		}
		s.progress.Touch(store.PlaybackProgress{
			UserID: u.Id, ItemID: it.Id,
			PositionTicks: pos, DurationTicks: it.RunTimeTicks,
			Played: played, PlayInc: playInc,
			LastPlayed: now.Unix(), Updated: now.UnixMilli(),
		})
	}

	ci := identifyClient(r)
	s.trace.add(map[string]any{
		"ts": now.UnixMilli(), "path": path, "client": ci.Client,
		"device": ci.Device, "session": body.PlaySessionId,
		"item": it.Name, "position": body.PositionTicks, "pos": pos,
		"paused": body.IsPaused, "method": body.PlayMethod,
	})
	row := store.SessionRow{
		ID: body.PlaySessionId, UserID: u.Id, UserName: u.Name,
		DeviceID: ci.DeviceID, Client: ci.Client, ClientVersion: ci.Version,
		DeviceName: ci.Device, ItemID: it.Id, ItemName: it.Name,
		PositionTicks: pos, RuntimeTicks: it.RunTimeTicks,
		IsPaused: body.IsPaused, PlayMethod: body.PlayMethod, IP: ci.IP,
		Started: now.Unix(), Updated: now.Unix(),
	}
	if row.ID == "" {
		// 有的客户端不发 PlaySessionId：用 用户+设备 生成稳定 ID。
		row.ID = u.Id + ":" + ci.DeviceID
	}

	if stopped {
		_ = s.db.CloseSession(r.Context(), row.ID)
		s.recordActivity(r, u.Id, u.Name, "play_stop", it.Id, it.Name, "")
		s.publish(Event{Type: "session", SessionID: row.ID})
		return
	}
	if started || body.PositionTicks == 0 {
		s.recordActivity(r, u.Id, u.Name, "play_start", it.Id, it.Name, body.PlayMethod)
	}
	_ = s.db.UpsertSession(r.Context(), row)
	s.publish(Event{Type: "session", SessionID: row.ID})
}

// activeSessions 取当前会话，并把同一会话的 started 保留（DB 层已处理）。
func (s *Server) activeSessions(r *http.Request) ([]store.SessionRow, error) {
	if s.db == nil {
		return nil, nil
	}
	return s.db.ActiveSessions(r.Context(), time.Now().Add(-sessionIdle).Unix())
}

// handleSessions 对应 GET /emby/Sessions：返回正在播放的会话（Emby 结构）。
func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	rows, err := s.activeSessions(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load sessions")
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"Id":                 row.ID,
			"UserId":             row.UserID,
			"UserName":           row.UserName,
			"Client":             row.Client,
			"DeviceName":         row.DeviceName,
			"DeviceId":           row.DeviceID,
			"ApplicationVersion": row.ClientVersion,
			"PlayMethod":         row.PlayMethod,
			"NowPlayingItem": func() map[string]any {
				np := map[string]any{"Id": row.ItemID, "Name": row.ItemName}
				if c, err := s.db.ItemContextOf(r.Context(), row.ItemID); err == nil && c.Kind == "Episode" {
					np["Type"] = "Episode"
					np["SeriesName"] = c.SeriesName
					np["SeriesId"] = c.SeriesID
					np["SeasonNumber"] = c.SeasonNumber
					np["IndexNumber"] = c.EpisodeNumber
				}
				return np
			}(),
			"PlayState": map[string]any{
				"PositionTicks": row.PositionTicks,
				"IsPaused":      row.IsPaused,
			},
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSessionsCapabilities 某些客户端在播放前会查能力。
func (s *Server) handleSessionsCapabilities(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// writeActiveSessions 供 Web 面板使用：比 Emby 结构多带百分比与时间。
func (s *Server) writeActiveSessions(w http.ResponseWriter, r *http.Request) {
	rows, err := s.activeSessions(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load sessions")
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		pct := 0.0
		if row.RuntimeTicks > 0 {
			pct = float64(row.PositionTicks) / float64(row.RuntimeTicks) * 100
		}
		// 剧集：补上剧名与季集号，否则面板只显示“第 N 集”。
		seriesName, season, episode, title := "", 0, 0, row.ItemName
		if c, err := s.db.ItemContextOf(r.Context(), row.ItemID); err == nil && c.Kind == "Episode" {
			seriesName, season, episode = c.SeriesName, c.SeasonNumber, c.EpisodeNumber
			if seriesName != "" {
				title = fmt.Sprintf("%s S%02dE%02d · %s", seriesName, season, episode, row.ItemName)
			}
		}
		out = append(out, map[string]any{
			"Id": row.ID, "UserId": row.UserID, "UserName": row.UserName,
			"ItemId": row.ItemID, "ItemName": row.ItemName, "Title": title,
			"SeriesName": seriesName, "SeasonNumber": season, "EpisodeNumber": episode,
			"Client": row.Client, "Device": row.DeviceName, "Version": row.ClientVersion,
			"ClientKind": guessClientKind(row.Client, ""),
			"Started":    row.Started, "Updated": row.Updated,
			"IsPaused": row.IsPaused, "Percent": pct,
			"PositionTicks": row.PositionTicks, "RuntimeTicks": row.RuntimeTicks,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleActivityLog 兼容 Emby 的活动日志接口。
func (s *Server) handleActivityLog(w http.ResponseWriter, r *http.Request) {
	if u := userFrom(r); u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if s.db == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Items": []store.Activity{}, "TotalRecordCount": 0})
		return
	}
	limit := parseIntDefault(getCI(r, "Limit", "limit"), 100)
	list, err := s.db.Activities(r.Context(), limit, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load activities")
		return
	}
	if list == nil {
		list = []store.Activity{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"Items": list, "TotalRecordCount": len(list)})
}

// handleLogout 对应 POST /emby/Sessions/Logout：让当前令牌立即失效。
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	tok := tokenFrom(r)
	if tok != "" {
		s.memTokens.Delete(tok)
		if s.db != nil {
			_ = s.db.DeleteTokenByValue(r.Context(), tok)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
