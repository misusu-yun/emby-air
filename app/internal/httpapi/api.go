// Package httpapi 实现 Emby 兼容的 HTTP 层。
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"emby-air/internal/jobs"
	"emby-air/internal/logbuf"
	"emby-air/internal/model"
	"emby-air/internal/scanner"
	"emby-air/internal/store"
)

// Config 是 HTTP 层需要的运行参数。
type Config struct {
	ServerName string
	ServerId   string
	Port       int
	MediaRoot  string
	ImageCache string
	// CompatLog 记录未实现的 /emby/* 请求，便于后续补齐。
	CompatLog func(*http.Request)
	// AfterScan 在扫描完成后调用（传入库根目录），用于自动刮削新条目。
	AfterScan func(root string)
	// Jobs 任务队列（扫描/刮削）。
	Jobs *jobs.Manager
	// Logs 最近日志环形缓冲。
	Logs *logbuf.Buffer
	// RefreshAll 由外部提供：按所有媒体库路径扫描并刮削。返回 false 表示已有任务在跑。
	RefreshAll func() bool
	// PopcornMatch 是网易爆米花的客户端特征（小写子串，匹配 X-Emby-Client/User-Agent 等）。
	// 只有命中这些特征的请求才会启用兼容逻辑，VidHub/其它客户端完全不受影响。
	PopcornMatch []string
	// PopcornProxyStream=true 时，爆米花的播放请求由服务端代理直链（而不是 302）。
	PopcornProxyStream bool
	// RefreshAllScrape 与 RefreshAll 相同，但扫描后额外执行刮削（用于 Pansave 刷新）。
	RefreshAllScrape func() bool
	// ShowMissing 统计某部剧在 TMDB 有、本地没有的分集（真实缺失，不是空占位）。
	ShowMissing func(ctx context.Context, seriesID string, includeSpecials, includeUnaired bool) ([]model.Item, error)
	// StillFetcher 按需下载 TMDB 缩略图（单集 still_path），用于懒加载分集封面。
	StillFetcher func(ctx context.Context, tmdbPath string) ([]byte, error)
	// EnqueueLibraryScan 把整库扫描放进任务队列（新建/编辑媒体库后调用）。
	EnqueueLibraryScan func(libraryID, name, mode string) bool
	// EnqueuePaths 把 Pansave 通知的具体目录放进任务队列；由外部决定是否同时排刮削。
	EnqueuePaths func(paths []string, name string) bool
	// OnSettingsChanged 在 Web 保存 TMDB/代理/刮削设置后触发，用于重建运行时客户端。
	OnSettingsChanged func()
	// EnqueueItemJob 把单条重新扫描/刮削放进任务队列（便于任务中心与日志追踪）。
	EnqueueItemJob func(kind, itemID, name string) bool
	// RescanItem / RescoreItem 供 Web「影片管理」单条操作使用。
	RescanItem  func(ctx context.Context, id string) error
	RescoreItem func(ctx context.Context, id string) error
}

// memTokenEntry 是数据库写锁超时时的内存兜底令牌；有短 TTL，避免永久有效。
type memTokenEntry struct {
	user    *model.User
	expires time.Time
}

// Server 持有依赖。
type Server struct {
	db  *store.DB
	cfg Config
	// startedAt 是进程内 Server 的创建时间，供 /webui/api/debug/runtime 计算 UptimeSeconds。
	startedAt time.Time
	// images 是内置占位图（1×1 透明 PNG），避免每次请求重复构造。
	images imageAssets
	// logQ 是 CONTRACT_LOG 的 JSONL 日志通道；为 nil 表示不记录。
	logQ chan logChunk
	// memTokens 保存“数据库写不进去时”的临时令牌，保证登录不被扫描写锁阻塞。
	memTokens sync.Map
	logOnce   sync.Once
	// warmerOnce 保证搜索预热只启动一次（New + Handler 都会调用）。
	warmerOnce sync.Once
	// latestMu 缓存 Moonfin 首页 Latest 的轻量响应（10 秒），避免同一轮加载重复查库/重复编码。
	latestMu    sync.Mutex
	latestCache map[string]latestCacheEntry
	// statsMu 缓存媒体库统计：单集计数要扫库，每次切库重算会让页面卡死。
	statsMu         sync.Mutex
	statsAt         time.Time
	statsData       []store.MediaLibraryStats
	statsRefreshing bool
	// logWG 等待日志 goroutine 退出（closeLogWriter 用）。
	logWG sync.WaitGroup
	// scanRunning 防止并发扫描；set-by StartScan，由扫描协程清空。
	scanRunning atomic.Bool
	// lifeCtx 是后台任务的生命周期 context：New 创建，Close 取消。
	lifeCtx    context.Context
	lifeCancel context.CancelFunc
	// lifeWG 等待后台任务（扫描/搜索预热/统计）退出；lifeMu 保证 Close 之后不会再有新的 Add。
	lifeWG sync.WaitGroup
	lifeMu sync.Mutex
	closed bool
	// closeOnce 保证 Close 可重复调用（重复 Close 只刷一次进度，不重复等待/panic）。
	closeOnce sync.Once
	// progress 聚合客户端进度上报，定时批量落库。
	progress *progressAgg
	// bus 向 Web 管理页广播会话/活动事件。
	bus *eventBus
	// trace 是最近 50 条播放上报（有界内存，约 16KB），用于排查客户端行为差异。
	trace playbackTrace
	// guard 拦截会话首条异常小的进度（最多 256 个会话，约 24KB）。
	guard sessionGuard
	// stillLocks 是单集缩略图的按 item 下载锁（存在时间极短，数量 ≤ 并发请求数）。
	stillLocks sync.Map
	// streamClient 用于爆米花的代理播放（不设总超时，只做流式转发）。
	streamClient *http.Client
	// probe 记录最近 40 次鉴权请求的客户端特征，用于识别未知客户端。
	probe clientProbe
}

// StartScan 在后台跑一次目录扫描；已在跑时返回 false。
// 返回 true 时不等待完成，错误写日志（HTTP 语义是 204 “已开始”）。
func (s *Server) StartScan(root string) bool {
	if s.db == nil || root == "" {
		return false
	}
	if !s.scanRunning.CompareAndSwap(false, true) {
		return false
	}
	// 通过 startBackground 启动：Close 之后拒绝新扫描，且 Close 会等待它退出。
	if !s.startBackground(func() {
		// 退出前先清 scanRunning，保证 Close 返回时状态已复位。
		defer s.scanRunning.Store(false)
		res, err := scanner.Scan(s.lifeCtx, s.db, scanner.Options{Root: root, BaseRoot: s.cfg.MediaRoot, Incremental: true})
		if err != nil {
			log.Printf("扫描失败：%v", err)
			return
		}
		log.Printf("扫描完成：访问 %d 项，入库 %d 条，耗时 %s", res.Scanned, res.Indexed, res.Elapsed.Round(time.Millisecond))
		if s.cfg.AfterScan != nil {
			s.cfg.AfterScan(root)
		}
	}) {
		s.scanRunning.Store(false)
		return false
	}
	return true
}

// startBackground 在 Server 生命周期内启动后台任务：Close 之后不再启动，
// 且 Close 会等待已启动的任务退出。返回 false 表示服务已关闭。
func (s *Server) startBackground(fn func()) bool {
	s.lifeMu.Lock()
	if s.closed {
		s.lifeMu.Unlock()
		return false
	}
	s.lifeWG.Add(1)
	s.lifeMu.Unlock()
	go func() {
		defer s.lifeWG.Done()
		fn()
	}()
	return true
}

// scanStatus 返回当前扫描状态（"Idle" 或 "Running"）。
func (s *Server) scanStatus() string {
	if s.scanRunning.Load() {
		return "Running"
	}
	if s.cfg.Jobs != nil && s.cfg.Jobs.Busy() {
		return "Running"
	}
	return "Idle"
}

// New 构造 Server。
func New(db *store.DB, cfg Config) *Server {
	s := &Server{db: db, cfg: cfg, startedAt: time.Now(), progress: newProgressAgg(db), bus: newEventBus()}
	s.lifeCtx, s.lifeCancel = context.WithCancel(context.Background())
	s.progress.start()
	s.startSearchWarmer()
	s.warmLibraryStats()
	return s
}

// startSearchWarmer 在后台预热搜索索引：首次搜索不再等 8 秒全表扫描，
// 扫描写入后也会在 30 秒内自动重建。
func (s *Server) startSearchWarmer() {
	if s.db == nil {
		return
	}
	s.warmerOnce.Do(func() { s.startBackground(s.searchWarmerLoop) })
}

func (s *Server) searchWarmerLoop() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	ctx := s.lifeCtx
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.db.WarmSearchIndex(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// scanCloseTimeout 是 Close 等待后台任务的最长时间：扫描不响应取消时也不能永久阻塞退出。
const scanCloseTimeout = 10 * time.Second

// Close 停止后台任务并刷出聚合中的播放进度，进程退出前调用。可重复调用。
// 顺序：取消生命周期 context → 等待后台任务退出 → 最后刷进度。
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.lifeMu.Lock()
		s.closed = true
		s.lifeMu.Unlock()
		if s.lifeCancel != nil {
			s.lifeCancel()
		}
		// WaitGroup 在后台任务里 Done；用 goroutine 等待以便超时兜底。
		done := make(chan struct{})
		go func() {
			s.lifeWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(scanCloseTimeout):
			log.Printf("关闭超时：后台任务 %s 内未退出，继续关闭", scanCloseTimeout)
		}
		if s.progress != nil {
			s.progress.close()
		}
	})
}

// putMemToken 保存一个 10 分钟有效的内存兜底令牌（数据库写锁超时时使用）。
func (s *Server) putMemToken(token string, u *model.User) {
	if token == "" || u == nil {
		return
	}
	s.memTokens.Store(token, memTokenEntry{user: u, expires: time.Now().Add(10 * time.Minute)})
}

// dropMemTokensForUser 清理某用户的全部内存令牌（禁用/改密/权限变化时调用）。
func (s *Server) dropMemTokensForUser(userID string) {
	s.memTokens.Range(func(k, v any) bool {
		if e, ok := v.(memTokenEntry); ok && e.user != nil && e.user.Id == userID {
			s.memTokens.Delete(k)
		}
		return true
	})
}

// Handler 返回挂好路由的 http.Handler。
//
// 路由全部用 Go 1.22 的方法 + 通配符语法；每个端点显式声明方法，
// 避免把 POST 打到 GET 处理函数上。
func (s *Server) Handler() http.Handler {
	s.images = buildImageAssets()
	s.startLogWriter()
	if s.progress == nil {
		s.progress = newProgressAgg(s.db)
		s.progress.start()
	}
	if s.bus == nil {
		s.bus = newEventBus()
	}
	s.startSearchWarmer()
	return s.route()
}

// route 只构建路由表（不含一次性初始化），便于测试单独使用。
func (s *Server) route() http.Handler {

	mux := http.NewServeMux()

	// 系统
	mux.HandleFunc("GET /webui", s.handleWebUI)
	mux.HandleFunc("POST /webui/login", s.handleWebLogin)
	mux.HandleFunc("GET /webui/events", s.auth(s.handleWebEvents))
	mux.HandleFunc("GET /webui/api/sessions", s.auth(s.handleWebSessions))
	mux.HandleFunc("GET /webui/api/public-users", s.auth(s.requireAdmin(s.handlePublicUsersSetting)))
	mux.HandleFunc("POST /webui/api/public-users", s.auth(s.requireAdmin(s.handlePublicUsersSetting)))
	mux.HandleFunc("GET /webui/api/activities", s.auth(s.handleWebActivities))
	mux.HandleFunc("GET /webui/api/media", s.auth(s.handleWebMediaList))
	mux.HandleFunc("GET /webui/api/media/libraries", s.auth(s.handleWebLibraries))
	mux.HandleFunc("GET /webui/api/settings", s.auth(s.requireAdmin(s.handleWebSettings)))
	mux.HandleFunc("POST /webui/api/settings", s.auth(s.requireAdmin(s.handleWebSettings)))
	mux.HandleFunc("GET /webui/api/scan-schedule", s.auth(s.handleScanSchedule))
	mux.HandleFunc("POST /webui/api/scan-schedule", s.auth(s.handleScanSchedule))
	mux.HandleFunc("POST /webui/api/media/{Id}/size", s.auth(s.handleWebMediaSize))
	mux.HandleFunc("GET /webui/api/debug/playback", s.auth(s.requireAdmin(s.handlePlaybackTrace)))
	mux.HandleFunc("GET /webui/api/debug/clients", s.auth(s.requireAdmin(s.handleClientProbe)))
	// 运行时指标：部署后观察扫描期间的内存/goroutine 峰值。
	mux.HandleFunc("GET /webui/api/debug/runtime", s.auth(s.requireAdmin(s.handleRuntimeStats)))
	mux.HandleFunc("POST /webui/api/libraries/{Id}/order", s.auth(s.handleWebLibraryOrder))
	mux.HandleFunc("POST /webui/api/media/{Id}/rescan", s.auth(s.handleWebMediaRescan))
	mux.HandleFunc("POST /webui/api/media/{Id}/rescore", s.auth(s.handleWebMediaRescore))
	mux.HandleFunc("DELETE /webui/api/media/{Id}", s.auth(s.handleWebMediaDelete))
	// 只匹配根路径：Go 1.22 的 {$} 是精确匹配，避免和 /emby/ 兜底冲突。
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/webui", http.StatusFound)
	})
	mux.HandleFunc("GET /emby/System/Ping", s.handlePing)
	mux.HandleFunc("POST /emby/System/Ping", s.handlePing)
	mux.HandleFunc("GET /emby/System/Info/Public", s.handlePublicInfo)
	mux.HandleFunc("GET /emby/System/Info", s.auth(s.handleSystemInfo))

	// 用户与鉴权
	mux.HandleFunc("POST /emby/Users/AuthenticateByName", s.handleAuthenticateByName)
	mux.HandleFunc("POST /emby/Users/Authenticate", s.handleAuthenticateByName)
	mux.HandleFunc("GET /emby/Users/Me", s.auth(s.handleUsersMe))
	mux.HandleFunc("GET /emby/Users/{UserId}", s.auth(s.handleUserByID))
	// 部分客户端/工具（Pansave 等）会成对请求无前缀的 /Users/{uid} 做服务探测：
	// 同一个只读处理函数，不新增写操作。
	mux.HandleFunc("GET /Users/{UserId}", s.auth(s.handleUserByID))
	mux.HandleFunc("GET /emby/Users", s.auth(s.handleUsers))
	mux.HandleFunc("GET /emby/Users/{UserId}/Views", s.auth(s.handleViews))
	// 账号管理（管理员）
	mux.HandleFunc("POST /emby/Users/New", s.auth(s.requireAdmin(s.handleCreateUser)))
	mux.HandleFunc("DELETE /emby/Users/{UserId}", s.auth(s.requireAdmin(s.handleDeleteUser)))
	mux.HandleFunc("POST /emby/Users/{UserId}", s.auth(s.handleRenameUser))
	mux.HandleFunc("GET /emby/Users/{UserId}/Images/Primary", s.handleUserAvatar)
	mux.HandleFunc("POST /emby/Users/{UserId}/Images/Primary", s.auth(s.handleUserAvatar))
	mux.HandleFunc("POST /emby/Users/{UserId}/Policy", s.auth(s.requireAdmin(s.handleUpdatePolicy)))
	mux.HandleFunc("POST /emby/Users/{UserId}/Password", s.auth(s.handleUpdatePassword))

	// 条目
	mux.HandleFunc("GET /emby/Users/{UserId}/Items", s.auth(s.handleUserItems))
	mux.HandleFunc("GET /emby/Items", s.auth(s.handleItems))
	mux.HandleFunc("GET /emby/Items/{Id}", s.auth(s.handleItem))
	mux.HandleFunc("POST /emby/Items/{Id}", s.auth(s.handleUpdateItem))
	mux.HandleFunc("POST /emby/Items/{Id}/Refresh", s.auth(s.handleItemRefresh))
	mux.HandleFunc("DELETE /emby/Items/{Id}", s.auth(s.requireAdmin(s.handleDeleteItem)))
	mux.HandleFunc("GET /emby/Users/{UserId}/Items/Resume", s.auth(s.handleResume))
	mux.HandleFunc("GET /emby/Users/{UserId}/Items/Latest", s.auth(s.handleLatest))
	mux.HandleFunc("GET /emby/Items/{Id}/Images/{Type}", s.imageAuth(s.handleImage))
	mux.HandleFunc("HEAD /emby/Items/{Id}/Images/{Type}", s.imageAuth(s.handleImage))
	mux.HandleFunc("GET /emby/Items/{Id}/Images/{Type}/{rest...}", s.imageAuth(s.handleImage))
	mux.HandleFunc("HEAD /emby/Items/{Id}/Images/{Type}/{rest...}", s.imageAuth(s.handleImage))

	// 播放
	mux.HandleFunc("GET /emby/Items/{Id}/PlaybackInfo", s.auth(s.handlePlaybackInfo))
	mux.HandleFunc("POST /emby/Items/{Id}/PlaybackInfo", s.auth(s.handlePlaybackInfo))
	mux.HandleFunc("GET /emby/Audio/{Id}/stream", s.auth(s.handleVideoStream))
	mux.HandleFunc("HEAD /emby/Audio/{Id}/stream", s.auth(s.handleVideoStream))

	// 媒体库管理：让服务第一次可用（建库 + 扫描）不必手写 SQL。
	mux.HandleFunc("POST /emby/Library/Refresh", s.auth(s.requireAdmin(s.handleLibraryRefresh)))
	mux.HandleFunc("GET /emby/Sessions", s.auth(s.handleSessions))
	mux.HandleFunc("POST /emby/Sessions/Playing", s.auth(s.handleSessionPing))
	mux.HandleFunc("POST /emby/Sessions/Playing/Progress", s.auth(s.handleSessionPing))
	mux.HandleFunc("POST /emby/Sessions/Playing/Stopped", s.auth(s.handleSessionPing))
	mux.HandleFunc("POST /emby/Sessions/Logout", s.auth(s.handleLogout))
	mux.HandleFunc("POST /emby/Sessions/Capabilities", s.auth(s.handleSessionsCapabilities))
	mux.HandleFunc("POST /emby/Sessions/Capabilities/Full", s.auth(s.handleSessionsCapabilities))
	mux.HandleFunc("GET /emby/Admin/Jobs", s.auth(s.requireAdmin(s.handleAdminJobs)))
	mux.HandleFunc("POST /emby/Admin/Jobs", s.auth(s.requireAdmin(s.handleAdminJobs)))
	mux.HandleFunc("DELETE /emby/Admin/Jobs/{Id}", s.auth(s.requireAdmin(s.handleAdminJobCancel)))
	mux.HandleFunc("GET /emby/Admin/Logs", s.auth(s.requireAdmin(s.handleAdminLogs)))
	mux.HandleFunc("GET /emby/Admin/Fs", s.auth(s.requireAdmin(s.handleAdminFs)))
	mux.HandleFunc("POST /emby/Library/VirtualFolders/{Id}", s.auth(s.requireAdmin(s.handleLibraryUpdate)))
	mux.HandleFunc("POST /emby/Library/VirtualFolders/{Id}/Image", s.auth(s.requireAdmin(s.handleLibraryCover)))
	mux.HandleFunc("GET /emby/Library/VirtualFolders/{Id}/Image", s.handleLibraryCover)
	mux.HandleFunc("GET /emby/Items/Counts", s.auth(s.handleItemsCounts))
	mux.HandleFunc("GET /emby/Users/{UserId}/Items/Counts", s.auth(s.handleItemsCounts))
	mux.HandleFunc("GET /emby/Users/{UserId}/Items/{ItemId}", s.auth(s.handleUserItem))
	mux.HandleFunc("GET /emby/Users/{UserId}/Items/{ItemId}/UserData", s.auth(s.handleUserItemData))
	mux.HandleFunc("POST /emby/Users/{UserId}/Items/{ItemId}/UserData", s.auth(s.handleUserItemData))
	mux.HandleFunc("GET /emby/Shows/{Id}/Seasons", s.auth(s.handleShowSeasons))
	mux.HandleFunc("GET /emby/Shows/{Id}/Episodes", s.auth(s.handleShowEpisodes))
	mux.HandleFunc("GET /emby/Shows/NextUp", s.auth(s.handleNextUp))
	mux.HandleFunc("GET /emby/Items/{Id}/Similar", s.auth(s.handleSimilar))
	mux.HandleFunc("GET /emby/Items/{Id}/Ancestors", s.auth(s.handleItemAncestors))
	mux.HandleFunc("GET /emby/Items/{Id}/ThumbnailSet", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/Items/{Id}/SpecialFeatures", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/Moonfin/{rest...}", s.auth(s.handleMoonfinCompat))
	mux.HandleFunc("POST /emby/Moonfin/{rest...}", s.auth(s.handleMoonfinCompat))
	mux.HandleFunc("GET /emby/embywebsocket", s.auth(s.handleEmbyWebSocket))
	mux.HandleFunc("GET /emby/Items/{Id}/ThemeMedia", s.auth(s.handleEmptyPage))
	mux.HandleFunc("GET /emby/Items/{Id}/LocalTrailers", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/Items/{Id}/Intros", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/Persons", s.auth(s.handleEmptyPage))
	mux.HandleFunc("GET /emby/Genres", s.auth(s.handleEmptyPage))
	mux.HandleFunc("GET /emby/Studios", s.auth(s.handleEmptyPage))
	mux.HandleFunc("GET /emby/Plugins", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/ScheduledTasks", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/System/ActivityLog/Entries", s.auth(s.handleActivityLog))
	mux.HandleFunc("GET /emby/System/Endpoint", s.auth(s.handleEmptyList))
	mux.HandleFunc("GET /emby/Auth/Keys", s.auth(s.requireAdmin(s.handleAuthKeys)))
	mux.HandleFunc("POST /emby/Auth/Keys", s.auth(s.requireAdmin(s.handleAuthKeys)))
	mux.HandleFunc("DELETE /emby/Auth/Keys", s.auth(s.requireAdmin(s.handleAuthKeys)))
	mux.HandleFunc("POST /emby/Users/{UserId}/FavoriteItems/{ItemId}", s.auth(s.handleUserItemState))
	mux.HandleFunc("DELETE /emby/Users/{UserId}/FavoriteItems/{ItemId}", s.auth(s.handleUserItemState))
	mux.HandleFunc("POST /emby/Users/{UserId}/PlayedItems/{ItemId}", s.auth(s.handleUserItemState))
	mux.HandleFunc("DELETE /emby/Users/{UserId}/PlayedItems/{ItemId}", s.auth(s.handleUserItemState))
	mux.HandleFunc("GET /emby/DisplayPreferences/{Id}", s.auth(s.handleEmptyObject))
	mux.HandleFunc("POST /emby/DisplayPreferences/{Id}", s.auth(s.handleNoContent))
	mux.HandleFunc("/emby/", s.handleCompatFallback)

	// ---- Pansave / Emby 兼容补充 ----
	mux.HandleFunc("GET /emby/Users/Query", s.auth(s.handleUsers))
	mux.HandleFunc("GET /emby/Users/Public", s.handlePublicUsers)
	mux.HandleFunc("GET /emby/Shows/Missing", s.auth(s.handleShowsMissing))
	mux.HandleFunc("POST /emby/Library/Media/Updated", s.auth(s.requireAdmin(s.handleMediaUpdated)))
	mux.HandleFunc("GET /emby/Items/{Id}/Download", s.auth(s.handleVideoStream))
	mux.HandleFunc("HEAD /emby/Items/{Id}/Download", s.auth(s.handleVideoStream))
	mux.HandleFunc("GET /emby/Videos/{Id}/{Action}", s.auth(s.handleVideoAlias))
	mux.HandleFunc("HEAD /emby/Videos/{Id}/{Action}", s.auth(s.handleVideoAlias))
	// 无前缀兜底：Filmly 的 base URL 已带 /emby 时，/Videos/... 会被 Pansave 拦截并 302；
	// base 不带 /emby 时直接命中这里，不至于 404。不影响 /emby/Videos 路由。
	mux.HandleFunc("GET /Videos/{Id}/{Action}", s.auth(s.handleVideoAlias))
	mux.HandleFunc("HEAD /Videos/{Id}/{Action}", s.auth(s.handleVideoAlias))
	mux.HandleFunc("GET /emby/Library/MediaFolders", s.auth(s.handleMediaFolders))
	mux.HandleFunc("GET /emby/Library/SelectableMediaFolders", s.auth(s.handleSelectableMediaFolders))
	mux.HandleFunc("GET /emby/System/Configuration", s.auth(s.handleSystemConfiguration))
	mux.HandleFunc("GET /emby/Library/VirtualFolders", s.auth(s.handleVirtualFolders))
	// 第三方工具（pansave 等）用它列出媒体库，语义同 VirtualFolders，返回数组。
	mux.HandleFunc("GET /emby/Library/VirtualFolders/Query", s.auth(s.handleVirtualFolders))
	mux.HandleFunc("POST /emby/Library/VirtualFolders/Query", s.auth(s.handleVirtualFolders))
	mux.HandleFunc("POST /emby/Library/VirtualFolders", s.auth(s.requireAdmin(s.handleVirtualFolderAdd)))
	mux.HandleFunc("DELETE /emby/Library/VirtualFolders/{Id}", s.auth(s.requireAdmin(s.handleLibraryDelete)))
	mux.HandleFunc("GET /emby/Library/VirtualFolders/Paths", s.auth(s.handleLibraryPaths))

	// WebSocket 占位：本服务不支持实时推送，明确告知客户端升级协议。
	mux.HandleFunc("/embywebsocket", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusUpgradeRequired, "websocket not supported")
	})

	// normalizeDoubleEmbyPrefix 在 logRequests 内层：访问日志仍记录客户端原始路径（便于诊断）。
	return s.sniffClient(s.logRequests(normalizeDoubleEmbyPrefix(mux)))
}

// normalizeDoubleEmbyPrefix 兼容客户端把 baseURL 里的 /emby 又拼了一次的请求
// （网易爆米花播放时请求 /emby/emby/Videos/{id}/stream）。只去掉多余的第一个 /emby，
// 只处理一层，不做循环，RawQuery/header/body 原样保留。
func normalizeDoubleEmbyPrefix(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const doubled = "/emby/emby/"
		if !strings.HasPrefix(r.URL.Path, doubled) {
			next.ServeHTTP(w, r)
			return
		}
		// Clone 会深拷贝 URL，改写不会影响外层（日志/嗅探）看到的原始路径。
		r2 := r.Clone(r.Context())
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/emby")
		r2.URL.RawPath = ""
		next.ServeHTTP(w, r2)
	})
}

// ---- 鉴权 ----

type ctxKey int

const userCtxKey ctxKey = iota

// userFrom 返回由鉴权中间件放入的用户；未鉴权时为 nil。
func userFrom(r *http.Request) *model.User {
	u, _ := r.Context().Value(userCtxKey).(*model.User)
	return u
}

// tokenFrom 提取客户端令牌：header X-Emby-Token 优先，其次 query api_key。
func tokenFrom(r *http.Request) string {
	if t := strings.TrimSpace(r.Header.Get("X-Emby-Token")); t != "" {
		return t
	}
	if t := strings.TrimSpace(r.Header.Get("X-MediaBrowser-Token")); t != "" {
		return t
	}
	// Emby 客户端的 Authorization 形如：
	//   MediaBrowser Client="...", Device="...", Token="xxxx"
	//   Bearer xxxx
	if ah := strings.TrimSpace(r.Header.Get("Authorization")); ah != "" {
		if strings.HasPrefix(strings.ToLower(ah), "bearer ") {
			return strings.TrimSpace(ah[7:])
		}
		if i := strings.Index(strings.ToLower(ah), "token="); i >= 0 {
			v := ah[i+6:]
			v = strings.TrimSpace(strings.Trim(v, `"`))
			if j := strings.IndexAny(v, ", "); j >= 0 {
				v = v[:j]
			}
			return strings.Trim(v, `"`)
		}
	}
	if t := strings.TrimSpace(r.URL.Query().Get("api_key")); t != "" {
		return t
	}
	return ""
}

// requireAdmin 必须在 s.auth 之后使用。普通用户访问管理接口返回 403，
// 避免客户端把 401 当成“token 失效”而反复重登。
func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u := userFrom(r)
		if u == nil {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !u.IsAdmin {
			writeError(w, http.StatusForbidden, "admin required")
			return
		}
		next(w, r)
	}
}

// apiKeyUser 表示 Emby 风格 API Key 的虚拟主体（不是真实用户）。
func isAPIKeyUser(u *model.User) bool { return u != nil && u.Id == "apikey" }

// auth 是鉴权中间件：失败一律 401 JSON。
// 数据库不可用（nil）时保持 401，绝不降级为放行。
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := tokenFrom(r)
		if tok == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if s.db == nil {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		if v, ok := s.memTokens.Load(tok); ok {
			if e, ok := v.(memTokenEntry); ok && time.Now().Before(e.expires) {
				next(w, r.WithContext(context.WithValue(r.Context(), userCtxKey, e.user)))
				return
			}
			s.memTokens.Delete(tok)
		}
		u, err := s.db.UserByToken(r.Context(), tok)
		if err != nil || u == nil {
			// 回退：Emby 风格的 API Key（无用户上下文，按管理员处理）。
			if ok, kerr := s.db.ValidateAPIKey(r.Context(), tok); kerr == nil && ok {
				apiUser := &model.User{Id: "apikey", Name: "API", IsAdmin: true, HasPassword: false}
				next(w, r.WithContext(context.WithValue(r.Context(), userCtxKey, apiUser)))
				return
			}
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		if u.Policy != nil && u.Policy.IsDisabled {
			// 立即失效：客户端下次请求会重新登录并得到 401。
			_ = s.db.DeleteTokenByValue(r.Context(), tok)
			writeError(w, http.StatusUnauthorized, "user disabled")
			return
		}
		if !isAPIKeyUser(u) {
			// 只记录非静态资源；Web 轮询会很快把缓冲刷掉，导致看不到客户端的真实请求。
			p := r.URL.Path
			if !strings.HasPrefix(p, "/webui") && !strings.Contains(p, "/Images/") {
				s.probe.add(map[string]string{
					"client": truncateRunes(r.Header.Get("X-Emby-Client"), 40),
					"device": truncateRunes(r.Header.Get("X-Emby-Device-Name"), 40),
					"ua":     truncateRunes(r.UserAgent(), 80),
					"path":   truncateRunes(r.Method+" "+p, 80),
				})
			}
			if s.isPopcornClient(r) {
				log.Printf("爆米花请求：%s %s", r.Method, p)
			}
		}
		next(w, r.WithContext(context.WithValue(r.Context(), userCtxKey, u)))
	}
}

// ---- 输出辅助 ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError 是统一的 JSON 错误信封；任何错误路径都不得返回 HTML。
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg, "status": status})
}

// writeJSONStatus 用于“先写头再流式编码”的场景。
func writeJSONStatus(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
}

// errUserVisible 判断 store 错误是否属于“不存在”。
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errNotFoundSentinel) {
		return true
	}
	// store 层目前用普通 errors.New；兼容常见的措辞而不依赖具体包。
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no rows")
}

var errNotFoundSentinel = errors.New("not found")

// serverID 返回配置的 ServerId，缺省给一个稳定值。
func (s *Server) serverID() string {
	if s.cfg.ServerId != "" {
		return s.cfg.ServerId
	}
	return "00000000000000000000000000000001"
}

func constTimeEq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
