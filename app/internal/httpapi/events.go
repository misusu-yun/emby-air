package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"emby-air/internal/store"
)

// Event 是推给 Web 前端的实时事件。
type Event struct {
	Type      string          `json:"type"` // session | activity | library
	SessionID string          `json:"sessionId,omitempty"`
	Activity  *store.Activity `json:"activity,omitempty"`
}

// eventBus 是进程内广播：订阅者慢就丢事件，绝不阻塞请求。
type eventBus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func newEventBus() *eventBus { return &eventBus{subs: make(map[chan Event]struct{})} }

func (b *eventBus) Subscribe(size int) chan Event {
	ch := make(chan Event, size)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *eventBus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	if _, ok := b.subs[ch]; ok {
		delete(b.subs, ch)
		close(ch)
	}
	b.mu.Unlock()
}

func (b *eventBus) Publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- ev:
		default: // 订阅者积压：丢弃，下次刷新会补齐
		}
	}
}

// publish 是 Server 的便捷方法。
func (s *Server) publish(ev Event) {
	if s.bus != nil {
		s.bus.Publish(ev)
	}
}

// recordActivity 写一条活动并推送给 Web。
func (s *Server) recordActivity(r *http.Request, userID, userName, kind, itemID, itemName string, detail string) {
	s.recordActivityAs(r, userID, userName, kind, itemID, itemName, detail, "")
}

// recordActivityAs 允许调用方覆盖客户端名（例如 Web 登录统一记为 Emby Web UI）。
func (s *Server) recordActivityAs(r *http.Request, userID, userName, kind, itemID, itemName, detail, client string) {
	if s.db == nil {
		return
	}
	ci := identifyClient(r)
	if client != "" {
		ci.Client = client
		ci.Kind = guessClientKind(client, r.UserAgent())
	}
	a := store.Activity{
		Ts:       time.Now().UnixMilli(),
		UserID:   userID,
		UserName: userName,
		Kind:     kind,
		ItemID:   itemID,
		ItemName: itemName,
		Client:   ci.Client,
		Device:   ci.Device,
		IP:       ci.IP,
		Detail:   detail,
	}
	// 活动是旁路数据：写失败不能影响主流程。
	if err := s.db.AddActivity(r.Context(), a); err != nil {
		return
	}
	s.publish(Event{Type: "activity", Activity: &a})
}

// handleWebEvents 是 SSE 推送端点（仅管理员）。
// EventSource 不能自定义 header，所以允许 ?api_key=。
func (s *Server) handleWebEvents(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	// 用 ResponseController：它能穿透日志中间件的 ResponseWriter 包装。
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := s.bus.Subscribe(64)
	defer s.bus.Unsubscribe(ch)

	fmt.Fprint(w, ": connected\n\n")
	if err := rc.Flush(); err != nil {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			_ = rc.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			_ = rc.Flush()
		}
	}
}

// handleWebSessions 给 Web 面板返回正在播放列表（含百分比）。
func (s *Server) handleWebSessions(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	s.writeActiveSessions(w, r)
}

// handleWebActivities 给 Web 面板返回活动流（倒序分页）。
func (s *Server) handleWebActivities(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	if s.db == nil {
		writeJSON(w, http.StatusOK, []store.Activity{})
		return
	}
	limit := parseIntDefault(getCI(r, "Limit", "limit"), 100)
	before := int64(parseIntDefault(getCI(r, "before"), 0))
	list, err := s.db.Activities(r.Context(), limit, before)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load activities")
		return
	}
	if list == nil {
		list = []store.Activity{}
	}
	writeJSON(w, http.StatusOK, list)
}

// handlePlaybackTrace 返回最近 50 条播放上报（排查 VidHub 上报值用，仅管理员）。
func (s *Server) handlePlaybackTrace(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.trace.snapshot())
}
