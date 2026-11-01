// Command emby-air 启动 Emby 协议兼容服务器，并提供建号 / 加库 / 扫描 / 刮削的一次性入口。
//
// 用法：
//
//	emby-air                          启动服务
//	emby-air -healthcheck             探活（给 Docker healthcheck 用）
//	emby-air -create-user NAME -password PW [-admin]
//	emby-air -add-library NAME [-root DIR]  登记媒体库（LIBRARY_TYPE 取 movies/tvshows）
//	emby-air -scan [-root DIR]        前台扫描一次库
//	emby-air -scrape [-force] [-limit N]  前台刮削一轮（NFO 优先，TMDB 兜底）
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"emby-air/internal/httpapi"
	"emby-air/internal/imgcache"
	"emby-air/internal/jobs"
	"emby-air/internal/logbuf"
	"emby-air/internal/model"
	"emby-air/internal/scanner"
	"emby-air/internal/scraper"
	"emby-air/internal/settings"
	"emby-air/internal/store"
	"emby-air/internal/tmdb"
	"io"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(env(k, "")); err == nil {
		return v
	}
	return def
}

func main() {
	var (
		healthcheck = flag.Bool("healthcheck", false, "probe /emby/System/Ping and exit")
		createUser  = flag.String("create-user", "", "create a user with this name and exit")
		addLibrary  = flag.String("add-library", "", "register a library with this name and exit")
		libType     = flag.String("library-type", "", "library type: movies | tvshows | mixed (default movies)")
		libPaths    = flag.String("paths", "", "comma-separated media paths for the library (default MEDIA_ROOT)")
		createKey   = flag.String("create-apikey", "", "create an Emby-style API key with this note and exit")
		listKeys    = flag.Bool("list-apikeys", false, "list API keys and exit")
		deleteKey   = flag.String("delete-apikey", "", "delete the given API key and exit")
		password    = flag.String("password", "", "password for -create-user (empty = no password)")
		admin       = flag.Bool("admin", false, "make -create-user an administrator")
		scan        = flag.Bool("scan", false, "run one library scan in the foreground and exit")
		scrape      = flag.Bool("scrape", false, "run one scrape pass in the foreground and exit")
		force       = flag.Bool("force", false, "with -scrape: redo every item (ignore scrape_state)")
		limit       = flag.Int("limit", 0, "with -scrape: stop after N items (0 = all)")
		root        = flag.String("root", "", "media root (default MEDIA_ROOT)")
		workers     = flag.Int("workers", 0, "parallel workers (scan/scrape), default 2")
		writeRate   = flag.Int("write-rate", 0, "scan: max items written per second (default 300)")
		incremental = flag.Bool("incremental", false, "scan: skip titles whose files are unchanged")
		watch       = flag.Duration("watch", 0, "scan: repeat incremental scan every interval (e.g. 60s)")
		rate        = flag.Float64("rate", 0, "TMDB requests per second (default 4)")
		lang        = flag.String("lang", "", "TMDB language (default TMDB_LANG or zh-CN)")
		// 封面补齐：只给 image_ref 为空的 Movie/Series 重刮并下载图片；不自动运行。
		backfillCovers = flag.Bool("backfill-covers", false, "backfill missing covers for Movie/Series via TMDB and exit")
		coversLimit    = flag.Int("covers-limit", 100, "with -backfill-covers: process at most N items")
		coversKind     = flag.String("covers-kind", "movie", "with -backfill-covers: movie | series | all")
	)
	flag.Parse()

	dataDir := env("DATA_DIR", "./data")
	imageCacheDir := env("IMAGE_CACHE", filepath.Join(dataDir, "cache", "images"))
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("数据目录创建失败: %v", err)
	}
	dbPath := env("DB_PATH", filepath.Join(dataDir, "emby.db"))
	mediaRoot := env("MEDIA_ROOT", "/media")
	if *root != "" {
		mediaRoot = *root
	}

	if *healthcheck {
		os.Exit(healthCheck())
	}

	db, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer db.Close()
	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Minute)
	if err := db.Migrate(initCtx); err != nil {
		cancelInit()
		log.Fatalf("数据库迁移失败: %v", err)
	}
	cancelInit()

	// 首次启动自动创建管理员：ADMIN_USER / ADMIN_PASSWORD。
	// 仅在用户不存在时创建，绝不覆盖已有密码，避免重启把管理员密码改掉。
	serverMode := *createUser == "" && *addLibrary == "" && *createKey == "" && *deleteKey == "" &&
		!*scan && !*scrape && !*backfillCovers && !*healthcheck && !*listKeys
	if u := env("ADMIN_USER", ""); serverMode && u != "" {
		if created, err := ensureInitialAdmin(context.Background(), db, u, env("ADMIN_PASSWORD", "")); err != nil {
			log.Printf("初始管理员创建失败：%v", err)
		} else if created {
			log.Printf("已创建初始管理员 %s（请尽快在 Web 里修改密码）", u)
		}
	}

	// 历史条目的索引字段：后台补齐，不阻塞启动。
	// 只在服务模式执行：-scan/-scrape/-backfill-covers 等一次性 CLI 也会打开同一个 DB，
	// 如果这里再跑 Normalize/Dedupe/ANALYZE，会和正在服务的进程抢写锁（Web UI 卡住）。
	if serverMode {
		go func() {
			// 先修正库归属：两个库共用同一路径时，扫描会把剧集挂到电影库。
			if n, err := db.NormalizeLibraryParents(context.Background()); err != nil {
				log.Printf("库归属修正失败: %v", err)
			} else if n > 0 {
				log.Printf("已修正 %d 条条目的库归属", n)
			}
			// 顺序很重要：先做用户可见的重复合并（快），再做孤儿修复与索引回填（慢）。
			if n, err := db.DedupeItems(context.Background()); err != nil {
				log.Printf("启动合并重复条目失败: %v", err)
			} else if n > 0 {
				log.Printf("启动时合并重复条目 %d 条", n)
			}
			if n, err := db.RepairOrphanSeries(context.Background()); err != nil {
				log.Printf("修复孤儿剧集关系失败: %v", err)
			} else if n > 0 {
				log.Printf("已修复 %d 条孤儿的季/集归属", n)
			}
			if n, err := db.BackfillSeriesName(context.Background()); err != nil {
				log.Printf("剧名回填失败: %v", err)
			} else if n > 0 {
				log.Printf("剧名已回填 %d 条", n)
			}
			if n, err := db.BackfillNameIndex(context.Background()); err != nil {
				log.Printf("拼音索引补齐失败: %v", err)
			} else if n > 0 {
				log.Printf("拼音索引已补齐 %d 条", n)
			}
			// 统计信息每周重建一次：大表上 sqlite_stat1 退化会让递归/排序查询慢 10 倍。
			debug.FreeOSMemory() // 维护任务结束后把临时堆还给系统
			if last := db.GetSetting(context.Background(), "analyze_at"); last == "" || daysSince(last) >= 7 {
				if err := db.OptimizeStats(context.Background()); err != nil {
					log.Printf("ANALYZE 失败: %v", err)
				} else {
					_ = db.SetSetting(context.Background(), "analyze_at", time.Now().Format("2006-01-02"))
					log.Printf("SQLite 统计信息已重建（ANALYZE）")
				}
			}
			cleanImageCache(db, imageCacheDir)
			debug.FreeOSMemory()
		}()
	}

	switch {
	case *createUser != "":
		u, err := db.CreateUser(context.Background(), *createUser, *password, *admin)
		if err != nil {
			log.Fatalf("创建用户失败: %v", err)
		}
		fmt.Printf("已创建用户 %s (%s) 管理员=%v\n", u.Name, u.Id, u.IsAdmin)
		return
	case *addLibrary != "":
		views, err := db.GetViews(context.Background())
		if err != nil {
			log.Fatalf("添加媒体库失败: %v", err)
		}
		for _, v := range views {
			if strings.EqualFold(v.Name, *addLibrary) {
				fmt.Printf("媒体库 %q 已存在 (%s)\n", v.Name, v.Id)
				return
			}
		}
		ct := *libType
		if ct == "" {
			ct = env("LIBRARY_TYPE", "movies")
		}
		paths := []string{mediaRoot}
		if strings.TrimSpace(*libPaths) != "" {
			paths = nil
			for _, p := range strings.Split(*libPaths, ",") {
				if p = strings.TrimSpace(p); p != "" {
					paths = append(paths, p)
				}
			}
		}
		v := model.View{Id: scanner.ItemID("library/" + strings.ToLower(*addLibrary)),
			Name: *addLibrary, CollectionType: ct, SortOrder: len(views), Paths: paths}
		if err := db.UpsertViews(context.Background(), []model.View{v}); err != nil {
			log.Fatalf("添加媒体库失败: %v", err)
		}
		fmt.Printf("已注册媒体库 %q (%s) 类型=%s 路径=%v\n", v.Name, v.Id, ct, paths)
		return
	case *createKey != "":
		k, err := db.CreateAPIKey(context.Background(), *createKey)
		if err != nil {
			log.Fatalf("创建 API Key 失败: %v", err)
		}
		fmt.Printf("api_key=%s\n", k)
		return
	case *listKeys:
		keys, err := db.ListAPIKeys(context.Background())
		if err != nil {
			log.Fatalf("列出 API Key 失败: %v", err)
		}
		for _, k := range keys {
			fmt.Printf("%s\t%s\t%s\n", k.Key, k.Note, time.Unix(k.Created, 0).Format(time.RFC3339))
		}
		return
	case *deleteKey != "":
		if err := db.DeleteAPIKey(context.Background(), *deleteKey); err != nil {
			log.Fatalf("删除 API Key 失败: %v", err)
		}
		fmt.Println("deleted")
		return
	case *scan:
		roots := resolveRoots(db, *root, mediaRoot)
		if *watch > 0 {
			runWatch(db, roots, mediaRoot, *workers, *writeRate, *watch)
			return
		}
		for _, r := range roots {
			runScan(db, r.Path, mediaRoot, r.ViewID, *workers, *writeRate, *incremental, filepath.Join(dataDir, "logs", "scan.log"))
		}
		return
	case *scrape:
		runScrape(db, mediaRoot, dataDir, scrapeFlags{force: *force, limit: *limit,
			workers: *workers, rate: *rate, lang: *lang})
		return
	case *backfillCovers:
		runBackfillCovers(db, mediaRoot, dataDir, *coversKind, *coversLimit)
		return
	}

	serve(db, dataDir, mediaRoot)
}

type scrapeFlags struct {
	force   bool
	limit   int
	workers int
	rate    float64
	lang    string
}

// healthCheck 访问本地端口；成功返回 0。
func healthCheck() int {
	base := fmt.Sprintf("http://127.0.0.1:%d/emby/System/Ping", envInt("PORT", 8097))
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get(base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", res.StatusCode)
		return 1
	}
	return 0
}

// runScan 扫描单个库根目录。baseRoot 是计算条目 ID 的固定根（MEDIA_ROOT）：
// 库路径是 MEDIA_ROOT 的子目录时，Root 只用来 walk，ID 必须相对 baseRoot 生成，
// 否则同一文件会因“扫描入口不同”得到两套 ID（重复条目、丢播放进度）。
func runScan(db *store.DB, root, baseRoot, libraryID string, workers, writeRate int, incremental bool, logPath string) {
	// 进度条：交互终端用单行覆盖，重定向时按行输出，方便落日志。
	isTTY := false
	if fi, err := os.Stdout.Stat(); err == nil {
		isTTY = fi.Mode()&os.ModeCharDevice != 0
	}
	var logFile *os.File
	if logPath != "" {
		if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			logFile = f
			defer f.Close()
		}
	}
	lastLog := time.Now()

	progress := func(p scanner.Progress) {
		pct := 0.0
		if p.Total > 0 {
			pct = float64(p.Titles) / float64(p.Total) * 100
		}
		bar := progressBar(pct, 28)
		line := fmt.Sprintf("%s %5.1f%%  %d/%d 标题  条目 %d  文件 %d  %.0f 条/s  RSS %dMB  %s",
			bar, pct, p.Titles, p.Total, p.Items, p.Files, p.Rate, p.RSSBytes/1024/1024, p.ETA.Round(time.Second))
		if isTTY {
			fmt.Printf("\r\033[2K%s", line)
		} else {
			fmt.Println(line)
		}
		if logFile != nil && time.Since(lastLog) >= 2*time.Second {
			lastLog = time.Now()
			rec, _ := json.Marshal(map[string]any{
				"ts": time.Now().UTC().Format(time.RFC3339), "phase": p.Phase,
				"pct": pct, "titles": p.Titles, "total": p.Total, "items": p.Items,
				"files": p.Files, "dirs": p.Dirs, "skipped": p.Skipped,
				"rate": p.Rate, "eta_s": p.ETA.Seconds(), "rss_mb": p.RSSBytes / 1024 / 1024,
				"elapsed_s": p.Elapsed.Seconds(),
			})
			fmt.Fprintln(logFile, string(rec))
		}
	}

	res, err := scanner.Scan(context.Background(), db, scanner.Options{Root: root, BaseRoot: baseRoot, Workers: workers, WriteRate: writeRate, Incremental: incremental, LibraryID: libraryID, Progress: progress})
	if isTTY {
		fmt.Print("\r\033[2K")
	}
	if err != nil {
		log.Fatalf("扫描失败: %v", err)
	}
	fmt.Printf("扫描完成：访问 %d 项，识别 %d 个 strm，入库 %d 条（剧 %d / 季 %d / 集 %d / 影 %d），耗时 %s（写库 %s）\n",
		res.Scanned, res.Matched, res.Indexed, res.Series, res.Seasons, res.Episodes, res.Movies,
		res.Elapsed.Round(time.Millisecond), res.WriteTime.Round(time.Millisecond))
}

// progressBar 生成 ████░░░░ 形式的进度条。
func progressBar(pct float64, width int) string {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func runScrape(db *store.DB, root, dataDir string, f scrapeFlags) {
	client := tmdb.New(tmdb.Options{
		APIKey:       env("TMDB_API_KEY", ""),
		BaseURL:      env("TMDB_BASE_URL", ""),
		ImageBaseURL: env("TMDB_IMAGE_BASE_URL", ""),
		Language:     firstNonEmpty(f.lang, env("TMDB_LANG", "zh-CN")),
		CacheDir:     filepath.Join(dataDir, "cache", "tmdb"),
	})
	if !client.Enabled() {
		log.Print("刮削：未配置 TMDB_API_KEY，仅使用 NFO/本地元数据")
	}
	if f.force {
		if err := db.ResetScrapeState(context.Background()); err != nil {
			log.Fatalf("刮削：重置状态失败: %v", err)
		}
	}
	w := f.workers
	if w <= 0 {
		w = scraper.NumWorkers()
	}
	s := scraper.New(db, client, scraper.Options{
		Root:     root,
		ImageDir: filepath.Join(dataDir, "cache", "images"),
		ServerID: env("SERVER_ID", "00000000000000000000000000000001"),
		Force:    f.force, ForceImages: f.force,
		Workers:    w,
		RatePerSec: f.rate,
		Batch:      200,
	})
	if f.limit > 0 {
		s = s.WithLimit(f.limit)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	res, err := s.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("刮削失败: %v", err)
	}
	fmt.Printf("刮削完成：处理 %d，NFO %d，TMDB %d，未找到 %d，失败 %d，图片 %d，耗时 %s\n",
		res.Processed, res.FromNFO, res.FromTMDB, res.NotFound, res.Failed, res.Images,
		res.Elapsed.Round(time.Millisecond))
}

// coverKinds 把 -covers-kind 映射成 items.kind 白名单。
func coverKinds(kind string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "movie", "movies":
		return []string{"Movie"}, nil
	case "series", "tvshows", "show", "shows":
		return []string{"Series"}, nil
	case "all", "both":
		return []string{"Movie", "Series"}, nil
	}
	return nil, fmt.Errorf("未知的 -covers-kind %q（可选 movie | series | all）", kind)
}

// runBackfillCovers 是封面补齐 CLI：只针对 image_ref 为空的 Movie/Series，
// 逐条复用 scraper.RunItem 重新刮削并下载海报/背景图（不新写 TMDB 下载逻辑）。
// 不自动运行、不新增 HTTP 端点；Series 会连带处理季/集，可能较慢，所以逐条打日志。
func runBackfillCovers(db *store.DB, mediaRoot, dataDir, kind string, limit int) {
	kinds, err := coverKinds(kind)
	if err != nil {
		log.Fatalf("封面补齐：%v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ids, err := db.MissingCoverIDs(ctx, kinds, limit)
	if err != nil {
		log.Fatalf("封面补齐：查询缺封面条目失败: %v", err)
	}
	if len(ids) == 0 {
		fmt.Printf("封面补齐：没有缺少封面的条目（类型 %v）\n", kinds)
		return
	}
	client := tmdb.New(tmdb.Options{
		APIKey:       env("TMDB_API_KEY", ""),
		BaseURL:      env("TMDB_BASE_URL", ""),
		ImageBaseURL: env("TMDB_IMAGE_BASE_URL", ""),
		Language:     env("TMDB_LANG", "zh-CN"),
		CacheDir:     filepath.Join(dataDir, "cache", "tmdb"),
	})
	if !client.Enabled() {
		log.Fatalf("封面补齐：未配置 TMDB_API_KEY，无法下载封面")
	}
	sc := scraper.New(db, client, scraper.Options{
		Root:        mediaRoot,
		ImageDir:    filepath.Join(dataDir, "cache", "images"),
		ServerID:    env("SERVER_ID", "00000000000000000000000000000001"),
		Force:       true,
		ForceImages: true,
		Workers:     2,
		RatePerSec:  15,
		Logger:      log.Default(),
	})

	fmt.Printf("封面补齐开始：类型 %v，共 %d 条（Ctrl-C 可中断）\n", kinds, len(ids))
	start := time.Now()
	var okN, noImage, failed int
	for i, id := range ids {
		if ctx.Err() != nil {
			fmt.Printf("封面补齐：已中断（已处理 %d/%d）\n", i, len(ids))
			break
		}
		res, err := sc.RunItem(ctx, id)
		if err != nil && ctx.Err() != nil {
			// Ctrl-C：RunItem 会因 ctx 取消返回 context.Canceled，不算失败。
			fmt.Printf("封面补齐：已中断（已处理 %d/%d）\n", i, len(ids))
			break
		}
		switch {
		case err != nil:
			failed++
			fmt.Printf("[%d/%d] 失败 %s：%v\n", i+1, len(ids), id, err)
		case res.Images == 0:
			// 没报错但一张图都没下到：TMDB 未匹配或该条目本身没有可用图片。
			noImage++
			fmt.Printf("[%d/%d] 无图 %s（TMDB %d / 未找到 %d）\n",
				i+1, len(ids), id, res.FromTMDB, res.NotFound)
		default:
			okN++
			fmt.Printf("[%d/%d] 成功 %s：图片 %d 张（TMDB %d / 未找到 %d）\n",
				i+1, len(ids), id, res.Images, res.FromTMDB, res.NotFound)
		}
	}
	fmt.Printf("封面补齐完成：成功 %d，无图 %d，失败 %d，共 %d 条，耗时 %s\n",
		okN, noImage, failed, len(ids), time.Since(start).Round(time.Second))
}

// cleanImageCache 每天检查一次、最多每周清理一次：删除已删除条目/媒体库的遗留图、
// 过期缩略图，并给 thumbs 设 512MB 上限。原图只在条目不存在时删除，避免误删有效海报。
func cleanImageCache(db *store.DB, dir string) {
	if db == nil || dir == "" {
		return
	}
	if last := db.GetSetting(context.Background(), "image_cache_clean_at"); last != "" && daysSince(last) < 7 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	stats, err := imgcache.Clean(ctx, dir, imgcache.Options{
		ServerID:      env("SERVER_ID", "00000000000000000000000000000001"),
		ThumbTTL:      30 * 24 * time.Hour,
		MaxThumbBytes: 512 << 20,
		ItemExists:    db.ItemExists,
		ViewExists:    db.ViewExists,
	})
	if err != nil {
		log.Printf("图片缓存清理失败: %v", err)
		return
	}
	_ = db.SetSetting(context.Background(), "image_cache_clean_at", time.Now().Format("2006-01-02"))
	if stats.Removed > 0 {
		log.Printf("图片缓存清理：删除 %d 个文件，释放 %.1f MB", stats.Removed, float64(stats.Freed)/(1<<20))
	}
}

func serve(db *store.DB, dataDir, mediaRoot string) {
	port := envInt("PORT", 8097)
	imageCacheDir := env("IMAGE_CACHE", filepath.Join(dataDir, "cache", "images"))

	// Web 设置保存后重建客户端；运行中的任务继续使用旧客户端，新任务使用新配置。
	var runtimeMu sync.RWMutex
	var runtimeCfg settings.Settings
	var tmdbClient *tmdb.Client
	newTMDBClient := func(cfg settings.Settings) *tmdb.Client {
		return tmdb.New(tmdb.Options{
			APIKey:       cfg.TMDBAPIKey,
			BaseURL:      env("TMDB_BASE_URL", ""),
			ImageBaseURL: env("TMDB_IMAGE_BASE_URL", ""),
			Language:     cfg.TMDBLang,
			CacheDir:     filepath.Join(dataDir, "cache", "tmdb"),
			Proxy:        cfg.Proxy,
			NoProxy:      cfg.NoProxy,
		}).WithRate(float64(cfg.ScrapeRate))
	}
	reloadSettings := func() {
		cfg := settings.Load(context.Background(), db)
		client := newTMDBClient(cfg)
		runtimeMu.Lock()
		runtimeCfg = cfg
		tmdbClient = client
		runtimeMu.Unlock()
	}
	currentSettings := func() settings.Settings {
		runtimeMu.RLock()
		defer runtimeMu.RUnlock()
		return runtimeCfg
	}
	currentTMDBClient := func() *tmdb.Client {
		runtimeMu.RLock()
		defer runtimeMu.RUnlock()
		return tmdbClient
	}
	reloadSettings()
	go func() {
		t := time.NewTicker(24 * time.Hour)
		defer t.Stop()
		for range t.C {
			cleanImageCache(db, imageCacheDir)
		}
	}()

	// 日志环形缓冲：Web 面板只看最近 N 行，避免无限增长。
	logRing := logbuf.New(2000)
	log.SetOutput(io.MultiWriter(os.Stderr, logRing))

	// heavyMu 串行化所有“重任务”（扫描/刮削/单条重扫重刮/刷新扫描/定时扫描）。
	// SQLite 是单写者：Pansave 的 Library/Refresh、Media/Updated 触发的 refreshAll
	// 与任务队列并发写库时，前端 /Items/Latest 这类查询会被写锁拖慢（反代日志里的 499）。
	// 获取顺序约定：先 heavyMu 再动数据库/scraper，且不得在持锁时再次获取（非重入锁）。
	var heavyMu sync.Mutex

	// 任务队列：扫描/刮削统一排队，串行执行（SQLite 单写者）。
	var mgr *jobs.Manager
	// HDD 休眠保护：HDD_GUARD=1 时，后台扫描/自动刮削只在允许的时间窗口内运行，
	// 窗口外直接拒绝，避免把机械盘唤醒。HDD_SCAN_WINDOW=02:00-05:00（本地时间）。
	hddGuard := env("HDD_GUARD", "0") == "1"
	scanWindow := env("HDD_SCAN_WINDOW", "02:00-05:00")
	inScanWindow := func(t time.Time) bool {
		if !hddGuard {
			return true
		}
		parts := strings.SplitN(scanWindow, "-", 2)
		if len(parts) != 2 {
			return false
		}
		now := t.Format("15:04")
		if parts[0] <= parts[1] {
			return now >= parts[0] && now <= parts[1]
		}
		return now >= parts[0] || now <= parts[1] // 跨午夜
	}
	var itemRescan func(ctx context.Context, id string) (string, error)
	var itemRescore func(ctx context.Context, id string) (string, error)
	mgr = jobs.New(func(ctx context.Context, j *jobs.Job) error {
		var roots []string
		libID, libName := "", ""
		if len(j.Paths) > 0 {
			// Pansave 定向通知：只处理它给出的标题目录。
			for _, p := range j.Paths {
				if p = strings.TrimSpace(p); p != "" {
					roots = append(roots, strings.TrimRight(p, "/"))
				}
			}
			libName = j.LibraryName
		} else if j.LibraryId != "" {
			if views, err := db.GetViews(ctx); err == nil {
				for _, v := range views {
					if v.Id != j.LibraryId {
						continue
					}
					libID, libName = v.Id, v.Name
					for _, p := range v.Paths {
						if p = strings.TrimSpace(p); p != "" {
							roots = append(roots, strings.TrimRight(p, "/"))
						}
					}
				}
			}
		}
		if len(roots) == 0 {
			roots = []string{mediaRoot}
		}
		// 真正开始工作前拿 heavyMu：扫描/刮削/单条重扫/重刮都会批量写库，
		// 与 refreshAll、定时扫描互斥，避免并发写库。
		// 注意：下面扫描/刮削/单条重扫/单条重刮各分支都直接开工，不再重复加锁；
		// 持锁期间也不能调用 refreshAll（refreshAll 自己会加锁，同一互斥锁不可重入）。
		heavyMu.Lock()
		defer heavyMu.Unlock()
		if j.Kind == jobs.KindIndex {
			store.SetSearchPaused(true)
			defer store.SetSearchPaused(false)
			n, err := db.RebuildNameIndex(ctx, func(done, total int) {
				mgr.Update(j.Id, total, done, "重建拼音索引")
			})
			if err != nil {
				return err
			}
			_ = db.SetSetting(context.Background(), "name_index_at", time.Now().Format("2006-01-02 15:04"))
			log.Printf("拼音索引全量重建完成：检查 %d 条", n)
			return nil
		}
		if j.Kind == jobs.KindRescan || j.Kind == jobs.KindRescore {
			label := j.LibraryName
			if j.Kind == jobs.KindRescan {
				if itemRescan == nil {
					return fmt.Errorf("扫描器未就绪")
				}
				msg, err := itemRescan(ctx, j.ItemId)
				if err != nil {
					log.Printf("重新扫描失败：%s：%v", label, err)
					return err
				}
				log.Printf("重新扫描完成：%s %s", label, msg)
				return nil
			}
			if itemRescore == nil {
				return fmt.Errorf("刮削器未就绪")
			}
			msg, err := itemRescore(ctx, j.ItemId)
			if err != nil {
				log.Printf("重新刮削失败：%s：%v", label, err)
				return err
			}
			log.Printf("重新刮削完成：%s %s", label, msg)
			return nil
		}
		if j.Kind == scannerKindScan() {
			if !inScanWindow(time.Now()) {
				return fmt.Errorf("HDD 保护已开启：仅在 %s 允许扫描（当前 %s）", scanWindow, time.Now().Format("15:04"))
			}
			store.SetSearchPaused(true)
			defer func() {
				store.SetSearchPaused(false)
				debug.FreeOSMemory() // 扫描结束把临时堆还给系统，避免“内存不回落”
			}()
			modeText := "增量"
			if j.Mode == jobs.ModeFull {
				modeText = "全量"
			}
			log.Printf("扫描开始：%s（%s）根目录 %s", libName, modeText, strings.Join(roots, ", "))
			lastLog := time.Now()
			indexed := 0
			changed := 0
			var elapsed time.Duration
			var pathIndex map[string]store.PathEntry
			loadedIndex := false
			loadIndex := func() {
				if loadedIndex || len(j.Paths) == 0 || j.Mode == jobs.ModeFull {
					return
				}
				loadedIndex = true
				// 只载入通知目录下的路径；多个目录共用同一份小索引。
				relRoots := make([]string, 0, len(roots))
				for _, root := range roots {
					if rel, err := filepath.Rel(mediaRoot, root); err == nil {
						relRoots = append(relRoots, rel)
					}
				}
				if idx, err := db.LoadPathIndexUnder(ctx, relRoots); err == nil {
					pathIndex = idx
				}
			}
			for _, root := range roots {
				// 库外路径绝不扫描/删除（旧版本曾把 /media/人工核实区 带进索引）。
				rootLibID := libID
				if len(j.Paths) > 0 {
					rootLibID = viewIDForTitle(db, mediaRoot, root)
					if rootLibID == "" {
						log.Printf("定向跳过库外路径：%s", root)
						continue
					}
				}
				// 目录已被整体删除：没有可遍历的根，直接按相对前缀删除数据库记录。
				if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
					rel, relErr := filepath.Rel(mediaRoot, root)
					if relErr == nil {
						if n, derr := db.DeletePathPrefix(ctx, rel, mediaRoot); derr != nil {
							log.Printf("定向删除失败：%s：%v", root, derr)
						} else if n > 0 {
							log.Printf("定向删除：%s（%d 条路径）", root, n)
						}
					}
					continue
				}
				loadIndex()
				res, err := scanner.Scan(ctx, db, scanner.Options{
					Root: root, BaseRoot: mediaRoot, LibraryID: rootLibID, Incremental: j.Mode != jobs.ModeFull,
					Index:       pathIndex,
					RootIsTitle: len(j.Paths) > 0 && scanner.IsTitleDir(root),
					ForceParse:  len(j.Paths) > 0,
					Progress: func(p scanner.Progress) {
						mgr.Update(j.Id, p.Total, p.Titles, "扫描中")
						// 进度日志按 3 秒节流，避免大库刷屏占内存。
						if time.Since(lastLog) >= 3*time.Second {
							lastLog = time.Now()
							log.Printf("扫描中：%s 已处理 %d/%d 个标题（入库 %d 条，内存 %.0fMB）", libName, p.Titles, p.Total, p.Items, float64(p.RSSBytes)/1024/1024)
						}
					},
				})
				if err != nil {
					return err
				}
				indexed += res.Indexed
				changed += res.Changed
				elapsed += res.Elapsed
			}
			// 定向扫描只动几个标题目录，父级归属已由 viewIDForTitle 指定；
			// 不再对全库做 Normalize/Dedupe，避免每次 Pansave 通知都扫一遍 15 万行。
			if len(j.Paths) == 0 {
				// 即使被取消也先合并一次：取消发生在半途，可能留下同一媒体的多条临时记录。
				// 只动数据库，不触碰任何媒体文件。
				// 多库共用同一根目录时，扫描会把条目挂到当前库；按类型归位。
				if _, nerr := db.NormalizeLibraryParents(context.Background()); nerr != nil {
					log.Printf("库归属修正失败: %v", nerr)
				}
				if n, derr := db.DedupeItems(context.Background()); derr == nil && n > 0 {
					log.Printf("扫描后合并重复条目 %d 条", n)
				}
			}
			// 扫描结束立即补齐拼音：清空搜索索引旧缓存，扫描完即可按全拼/首字母搜索。
			if n, err := db.FillMissingNameIndex(ctx, 2000); err != nil {
				log.Printf("拼音索引补齐失败：%v", err)
			} else if n > 0 {
				log.Printf("拼音索引已补齐 %d 条", n)
			}
			msg := "无变更"
			if changed > 0 {
				msg = fmt.Sprintf("检测到 %d 个变化，入库 %d 条", changed, indexed)
			}
			mgr.Update(j.Id, changed, changed, msg)
			log.Printf("任务完成：扫描 %s（变更 %d 个文件，入库 %d 条，耗时 %s）", libName, changed, indexed, elapsed.Round(time.Millisecond))
			if len(j.Paths) > 0 && changed > 0 {
				name := strings.TrimSuffix(j.LibraryName, " · 扫描") + " · 刮削"
				mgr.SubmitPaths(jobs.KindScrape, jobs.ModeIncremental, j.Paths, name)
				log.Printf("定向扫描有变化，刮削任务入队：%s", name)
			}
			return nil
		}
		client := currentTMDBClient()
		if client == nil || !client.Enabled() {
			return fmt.Errorf("未配置 TMDB_API_KEY")
		}
		cfg := currentSettings()
		// 刮削期间暂停搜索索引重建：每条 ApplyMeta 都会把索引标脏，
		// 否则 Web 搜索/预热会反复把 7 万行全量加载，造成 CPU 与内存尖峰（界面卡顿）。
		store.SetSearchPaused(true)
		defer store.SetSearchPaused(false)
		// 按媒体库类型决定刮削对象：电影库只刮 Movie，剧集库只刮 Series（一部剧=一条）。
		kinds := libraryScrapeKinds(ctx, db, libID)
		// 定向目录里的新集通常属于一部已经刮过的剧；仅靠 scrape_state 的增量过滤
		// 会跳过这部剧，导致新集没有简介/剧照。定向任务强制刷新这些标题目录。
		force := j.Mode == jobs.ModeFull || (len(j.Paths) > 0 && j.Kind == jobs.KindScrape)
		total, _ := db.CountScrapeCandidates(ctx, force, roots, kinds, store.DefaultMaxScrapeAttempts)
		if total > 0 {
			mgr.Update(j.Id, total, 0, "刮削中")
		}
		modeText := "增量"
		if j.Mode == jobs.ModeFull {
			modeText = "全量"
		} else if force {
			modeText = "定向"
		}
		log.Printf("刮削开始：%s（%s）待处理 %d 条", libName, modeText, total)
		if len(j.Paths) > 0 {
			kinds = nil // 定向目录可能同时含 Movie/Series，交给路径过滤
		}
		sc := scraper.New(db, client, scraper.Options{
			Root: mediaRoot, LibraryPaths: roots, Kinds: kinds, Force: force, ForceImages: force,
			UseNFO:   cfg.NFOWrite,
			ImageDir: filepath.Join(dataDir, "cache", "images"),
			ServerID: env("SERVER_ID", "00000000000000000000000000000001"),
			Workers:  2, RatePerSec: float64(cfg.ScrapeRate), Logger: log.Default(),
			OnItem:     func(name string) { mgr.Update(j.Id, -1, -1, "正在刮削："+name) },
			OnProgress: func(done int) { mgr.Update(j.Id, -1, done, "") },
		})
		res, err := sc.Run(ctx)
		if err != nil {
			return err
		}
		log.Printf("任务完成：刮削 %s（TMDB %d / 失败 %d，耗时 %s）", libName, res.FromTMDB, res.Failed, res.Elapsed.Round(time.Second))
		return nil
	})

	// 刮削器：运行时 TMDB 客户端 + 串行锁，供任务、自动刮削和按需图片共用。
	var compatMu sync.Mutex
	compatSeen := map[string]time.Time{}
	// 单条重新扫描 / 重新刮削（供任务队列与 Web 影片管理共用）。
	itemRescan = func(ctx context.Context, id string) (string, error) {
		row, err := db.ItemRow(ctx, id)
		if err != nil {
			return "", err
		}
		titleDir, viewID := titleDirFor(db, mediaRoot, row.Path)
		if titleDir == "" {
			return "", fmt.Errorf("无法定位标题目录")
		}
		log.Printf("重新扫描：%s（目录 %s）", row.Name, titleDir)
		if _, err := scanner.Scan(ctx, db, scanner.Options{
			Root: titleDir, BaseRoot: mediaRoot, LibraryID: viewID,
			RootIsTitle: true, ForceParse: true,
			Workers: 1, WriteRate: 0, BatchSize: 50, Incremental: false,
		}); err != nil {
			return "", err
		}
		return "已重新扫描 " + titleDir, nil
	}
	itemRescore = func(ctx context.Context, id string) (string, error) {
		row, err := db.ItemRow(ctx, id)
		if err != nil {
			return "", err
		}
		target := id
		if row.Type == "Episode" || row.Type == "Season" {
			it, err := db.GetItem(ctx, id)
			if err != nil || it == nil || it.SeriesId == "" {
				return "", fmt.Errorf("单集/季缺少所属剧集，无法刮削")
			}
			target = it.SeriesId
		}
		client := currentTMDBClient()
		if client == nil || !client.Enabled() {
			return "", fmt.Errorf("未配置 TMDB_API_KEY")
		}
		cfg := currentSettings()
		sc := scraper.New(db, client, scraper.Options{
			Root: mediaRoot, ImageDir: filepath.Join(dataDir, "cache", "images"),
			ServerID: env("SERVER_ID", "00000000000000000000000000000001"),
			Force:    true, ForceImages: true, Workers: 1, RatePerSec: float64(cfg.ScrapeRate), UseNFO: cfg.NFOWrite, Logger: log.Default(),
		})
		log.Printf("重新刮削：%s%s", row.Name, map[bool]string{true: "（连同所属剧集）", false: ""}[target != id])
		res, err := sc.RunItem(ctx, target)
		if err != nil {
			return "", err
		}
		msg := fmt.Sprintf("TMDB %d / 未找到 %d / 失败 %d / 图片 %d", res.FromTMDB, res.NotFound, res.Failed, res.Images)
		if res.Processed == 0 {
			return msg, fmt.Errorf("没有可刮削的内容")
		}
		return msg, nil
	}

	var scrapeMu sync.Mutex
	var refreshRunning atomic.Bool
	var triggerScrape func() // 在 doScrape 定义后赋值，避免前向引用
	refreshAll := func(withScrape bool) bool {
		if !refreshRunning.CompareAndSwap(false, true) {
			return false
		}
		go func() {
			defer refreshRunning.Store(false)
			// 与任务队列/定时扫描互斥：Pansave 刷新扫描不再和队列任务并发写库。
			// 后续可迁移到队列：本次只做串行化，不改变 HTTP 侧“已开始”的语义。
			heavyMu.Lock()
			defer heavyMu.Unlock()
			// 扫描期间暂停搜索索引重建：否则 30s 一次的 warmer 会把 7 万行反复全量加载。
			// 下面的 doScrape 在持锁+暂停索引的状态下直接执行（它内部不再加锁）。
			store.SetSearchPaused(true)
			defer store.SetSearchPaused(false)
			roots := resolveRoots(db, "", mediaRoot)
			for _, root := range roots {
				if _, err := scanner.Scan(context.Background(), db, scanner.Options{
					Root: root.Path, BaseRoot: mediaRoot, LibraryID: root.ViewID, Workers: 2, WriteRate: 0, BatchSize: 200, Incremental: true,
				}); err != nil {
					log.Printf("刷新扫描 %s 失败：%v", root.Path, err)
				}
			}
			db.NormalizeLibraryParents(context.Background())
			if withScrape && triggerScrape != nil {
				log.Printf("刷新扫描完成，开始刮削")
				triggerScrape()
			} else {
				log.Printf("刷新扫描完成（未自动刮削）")
			}
		}()
		return true
	}
	doScrape := func() {
		client := currentTMDBClient()
		cfg := currentSettings()
		if client == nil || !client.Enabled() || !cfg.AutoScrape {
			return
		}
		scrapeMu.Lock()
		defer scrapeMu.Unlock()
		sc := scraper.New(db, client, scraper.Options{
			Root:       mediaRoot,
			ImageDir:   filepath.Join(dataDir, "cache", "images"),
			ServerID:   env("SERVER_ID", "00000000000000000000000000000001"),
			Workers:    2,
			RatePerSec: float64(cfg.ScrapeRate),
			Logger:     log.Default(),
			UseNFO:     cfg.NFOWrite,
		})
		res, err := sc.Run(context.Background())
		if err != nil {
			log.Printf("自动刮削失败：%v", err)
			return
		}
		log.Printf("自动刮削完成：共 %d（NFO %d / TMDB %d / 未找到 %d / 失败 %d），图片 %d 张，耗时 %s",
			res.Processed, res.FromNFO, res.FromTMDB, res.NotFound, res.Failed, res.Images, res.Elapsed.Round(time.Second))
	}
	triggerScrape = func() { doScrape() }

	// 缺失分集统计：TMDB 每部剧只取一次详情，结果按（剧,选项）缓存 30 分钟，最多 300 部。
	type missingKey struct {
		series   string
		specials bool
		unaired  bool
	}
	type missingEntry struct {
		items []model.Item
		exp   time.Time
	}
	var missingMu sync.Mutex
	missingCache := map[missingKey]missingEntry{}
	showMissing := func(ctx context.Context, seriesID string, includeSpecials, includeUnaired bool) ([]model.Item, error) {
		key := missingKey{seriesID, includeSpecials, includeUnaired}
		missingMu.Lock()
		if e, ok := missingCache[key]; ok && time.Now().Before(e.exp) {
			missingMu.Unlock()
			return e.items, nil
		}
		missingMu.Unlock()

		name, tmdbID, ok := db.SeriesMeta(ctx, seriesID)
		client := currentTMDBClient()
		if !ok || tmdbID == 0 || client == nil || !client.Enabled() {
			return nil, nil
		}
		tv, err := client.SeriesDetail(ctx, tmdbID)
		if err != nil {
			return nil, err
		}
		local, err := db.SeriesEpisodes(ctx, seriesID, -1)
		if err != nil {
			return nil, err
		}
		have := map[[2]int]bool{}
		for _, ep := range local {
			have[[2]int{ep.ParentIndexNumber, ep.IndexNumber}] = true
		}
		today := time.Now().Format("2006-01-02")
		var out []model.Item
		for _, season := range tv.Seasons {
			if season.SeasonNumber == 0 && !includeSpecials {
				continue
			}
			if season.AirDate != "" && season.AirDate > today && !includeUnaired {
				continue
			}
			for n := 1; n <= season.EpisodeCount; n++ {
				if have[[2]int{season.SeasonNumber, n}] {
					continue
				}
				out = append(out, model.Item{
					Id:                fmt.Sprintf("missing-%s-%d-%d", seriesID, season.SeasonNumber, n),
					Name:              fmt.Sprintf("S%02dE%02d", season.SeasonNumber, n),
					Type:              "Episode",
					Path:              "",
					SeriesId:          seriesID,
					SeriesName:        name,
					ParentIndexNumber: season.SeasonNumber,
					IndexNumber:       n,
				})
			}
			if len(out) >= 500 {
				break
			}
		}
		// 缓存：简单容量控制，满了清空（条目通常很小）。
		missingMu.Lock()
		if len(missingCache) >= 300 {
			missingCache = map[missingKey]missingEntry{}
		}
		missingCache[key] = missingEntry{items: out, exp: time.Now().Add(30 * time.Minute)}
		missingMu.Unlock()
		return out, nil
	}

	srv := httpapi.New(db, httpapi.Config{
		ServerName:    env("SERVER_NAME", "emby-air"),
		ServerId:      env("SERVER_ID", "00000000000000000000000000000001"),
		Port:          port,
		MediaRoot:     mediaRoot,
		ImageCache:    imageCacheDir,
		SubtitleCache: filepath.Join(dataDir, "cache", "subtitles"),
		UpstreamHost:  env("UPSTREAM_HOST", ""),
		Jobs:          mgr,
		Logs:          logRing,
		CompatLog: func(r *http.Request) {
			// 兜底日志按路径去重：Moonfin/客户端轮询会反复打同一个接口，
			// 10 分钟内同路径只记一次，避免把 Web 日志刷满。
			key := r.Method + " " + r.URL.Path
			compatMu.Lock()
			if t, ok := compatSeen[key]; ok && time.Since(t) < 10*time.Minute {
				compatMu.Unlock()
				return
			}
			if len(compatSeen) > 300 {
				compatSeen = map[string]time.Time{}
			}
			compatSeen[key] = time.Now()
			compatMu.Unlock()
			log.Printf("兼容兜底：未实现 %s %s（已返回空数据）", r.Method, r.URL.Path)
		},
		// 手动扫描/库变更：只扫描入库，不自动刮削（刮削由用户显式触发或 Pansave 刷新）。
		AfterScan: func(root string) {},
		// 分集缩略图懒加载：只有客户端真的请求时才下载（w300，约 20~40KB）。
		// 网易爆米花：默认开启定向兼容（只命中特征串，不影响 VidHub/Infuse/Emby）。
		PopcornMatch:       popcornPatterns(),
		PopcornProxyStream: env("POPCORN_STREAM_MODE", "proxy") != "redirect",
		ShowMissing:        showMissing,
		StillFetcher: func(ctx context.Context, p string) ([]byte, error) {
			client := currentTMDBClient()
			if client == nil {
				return nil, fmt.Errorf("tmdb unavailable")
			}
			return client.DownloadImage(ctx, p, "w300")
		},
		EnqueueLibraryScan: func(libraryID, name, mode string) bool {
			m := jobs.ModeIncremental
			if mode == "full" {
				m = jobs.ModeFull
			}
			mgr.Submit(jobs.KindScan, m, libraryID, name)
			return true
		},
		// Pansave 定向通知只先排扫描；扫描确认有变化后再排对应的定向刮削。
		EnqueuePaths: func(paths []string, name string) bool {
			if len(paths) == 0 {
				return false
			}
			mgr.SubmitPaths(jobs.KindScan, jobs.ModeIncremental, paths, name+" · 扫描")
			return true
		},
		OnSettingsChanged: reloadSettings,
		EnqueueItemJob: func(kind, itemID, name string) bool {
			mgr.SubmitItem(jobs.Kind(kind), itemID, name)
			return true
		},
		RescanItem: func(ctx context.Context, id string) error {
			_, err := itemRescan(ctx, id)
			return err
		},
		// 单条重新刮削：强制重下元数据与图片。
		RescoreItem: func(ctx context.Context, id string) error {
			_, err := itemRescore(ctx, id)
			return err
		},
		// 扫描全部库。RefreshAll 只扫描；RefreshAllScrape 扫描后再刮削（Pansave 刷新用）。
		RefreshAll:       func() bool { return refreshAll(false) },
		RefreshAllScrape: func() bool { return refreshAll(true) },
	})

	// 定时扫描：默认关闭，由 Web「媒体库 → 扫描计划」设定每天几点执行一次。
	// 旧的 WATCH_INTERVAL（每 N 秒轮询）仅在显式设置时保留，用于兼容旧部署。
	go func() {
		lastRun := ""
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for {
			if hddGuard {
				<-tick.C
				continue // HDD 保护开启：不做任何后台自动扫描
			}
			sched := db.GetSetting(context.Background(), "scan_schedule")
			if sched != "" {
				now := time.Now()
				if now.Format("15:04") == sched && lastRun != now.Format("2006-01-02") {
					lastRun = now.Format("2006-01-02")
					log.Printf("定时扫描触发（%s）", sched)
					// 用闭包包住 defer：避免在 for 循环里累积 defer。
					func() {
						heavyMu.Lock()
						defer heavyMu.Unlock()
						store.SetSearchPaused(true)
						defer store.SetSearchPaused(false)
						roots := resolveRoots(db, "", mediaRoot)
						for _, root := range roots {
							res, err := scanner.Scan(context.Background(), db, scanner.Options{
								Root: root.Path, BaseRoot: mediaRoot, LibraryID: root.ViewID,
								Workers: 2, WriteRate: 0, BatchSize: 100, Incremental: true,
							})
							if err != nil {
								log.Printf("定时扫描 %s 失败：%v", root.Path, err)
								continue
							}
							log.Printf("定时扫描完成：%s 新增/更新 %d 条", root.Path, res.Indexed)
						}
						db.NormalizeLibraryParents(context.Background())
					}()
				}
			}
			<-tick.C
		}
	}()

	// 拼音索引定时重建：只读/写数据库，不触发媒体扫描；默认关闭，由 Web 设置页开启。
	// 与扫描共用任务队列，避免它和扫描同时写 SQLite。
	go func() {
		lastRun := ""
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for range tick.C {
			sched := db.GetSetting(context.Background(), "name_index_schedule")
			if sched == "" {
				continue
			}
			now := time.Now()
			day := now.Format("2006-01-02")
			if now.Format("15:04") != sched || lastRun == day {
				continue
			}
			lastRun = day
			mgr.Submit(jobs.KindIndex, jobs.ModeFull, "", "拼音索引")
			log.Printf("定时拼音索引重建已入队（%s）", sched)
		}
	}()

	if d := envDuration("WATCH_INTERVAL", 0); d > 0 && !hddGuard {
		go func() {
			log.Printf("旧版轮询扫描已启用：每 %s 扫描一次（建议改用定时计划）", d)
			for {
				if active, err := db.ActiveSessions(context.Background(), time.Now().Add(-60*time.Second).Unix()); err == nil && len(active) > 0 {
					time.Sleep(d)
					continue
				}
				// 与队列任务/Pansave 刷新互斥，并在扫描期间暂停搜索索引重建。
				// 用闭包包住 defer：避免在 for 循环里累积 defer。
				func() {
					heavyMu.Lock()
					defer heavyMu.Unlock()
					store.SetSearchPaused(true)
					defer store.SetSearchPaused(false)
					for _, root := range resolveRoots(db, "", mediaRoot) {
						_, _ = scanner.Scan(context.Background(), db, scanner.Options{
							Root: root.Path, BaseRoot: mediaRoot, LibraryID: root.ViewID,
							Workers: 2, WriteRate: 0, BatchSize: 100, Incremental: true,
						})
					}
					db.NormalizeLibraryParents(context.Background())
				}()
				time.Sleep(d)
			}
		}()
	}

	httpServer := &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("HTTP 服务已监听 :%d（媒体根目录 %s）", port, mediaRoot)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务异常退出: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	srv.Close() // 把内存里未落库的播放进度刷出去
	log.Print("服务已停止")
}

// ensureInitialAdmin 首次启动时按 ADMIN_USER 自动创建管理员。
// 语义：name 为空或用户已存在时什么都不做（返回 false），绝不覆盖已有密码；
// 用户不存在（store.ErrNotFound）才建号并返回 true；其它数据库错误原样返回。
func ensureInitialAdmin(ctx context.Context, db *store.DB, name, password string) (bool, error) {
	if name == "" {
		return false, nil
	}
	if _, _, err := db.UserWithHash(ctx, name); err == nil {
		return false, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	if _, err := db.CreateUser(ctx, name, password, true); err != nil {
		return false, err
	}
	return true, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// resolveRoots 决定扫描哪些目录：显式 -root 优先，否则用所有媒体库登记的路径。
type scanRoot struct {
	Path   string
	ViewID string
}

func resolveRoots(db *store.DB, flagRoot, mediaRoot string) []scanRoot {
	if flagRoot != "" {
		return []scanRoot{{Path: flagRoot}}
	}
	views, err := db.GetViews(context.Background())
	if err == nil {
		var out []scanRoot
		seen := map[string]bool{}
		for _, v := range views {
			for _, p := range v.Paths {
				if p != "" && !seen[p] {
					seen[p] = true
					out = append(out, scanRoot{Path: p, ViewID: v.Id})
				}
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return []scanRoot{{Path: mediaRoot}}
}

// runWatch 以固定间隔做增量扫描，模拟 Emby 的实时监控（轮询式，避免 inotify 上限）。
// baseRoot 是计算条目 ID 的固定根（MEDIA_ROOT），需与其它扫描入口保持一致。
func runWatch(db *store.DB, roots []scanRoot, baseRoot string, workers, writeRate int, interval time.Duration) {
	log.Printf("增量监听：每 %s 扫描 %v", interval, roots)
	for {
		start := time.Now()
		total := 0
		for _, root := range roots {
			res, err := scanner.Scan(context.Background(), db, scanner.Options{
				Root: root.Path, BaseRoot: baseRoot, LibraryID: root.ViewID, Workers: workers, WriteRate: writeRate, Incremental: true,
			})
			if err != nil {
				log.Printf("watch: scan %s failed: %v", root.Path, err)
				continue
			}
			total += res.Indexed
		}
		log.Printf("watch: indexed=%d duration=%s", total, time.Since(start).Round(time.Millisecond))
		time.Sleep(interval)
	}
}

// envDuration 读取 time.ParseDuration 形式的环境变量。
func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// scannerKindScan 避免在闭包里重复写类型判断。
// daysSince 返回日期字符串（2006-01-02）距今天数；解析失败按“需要重建”处理。
func daysSince(date string) int {
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		return 999
	}
	return int(time.Since(t).Hours() / 24)
}

// libraryScrapeKinds 根据媒体库 CollectionType 返回刮削对象类型。
// Emby 有效值：movies / tvshows；未知或未指定时两类都刮。
func libraryScrapeKinds(ctx context.Context, db *store.DB, libID string) []string {
	if libID == "" {
		return nil
	}
	views, err := db.GetViews(ctx)
	if err != nil {
		return nil
	}
	for _, v := range views {
		if v.Id != libID {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(v.CollectionType)) {
		case "movies":
			return []string{"Movie"}
		case "tvshows":
			return []string{"Series"}
		}
	}
	return nil
}

func scannerKindScan() jobs.Kind { return jobs.KindScan }

// viewIDForTitle 返回某个标题目录所属的媒体库 ID（取最长路径前缀）。
func viewIDForTitle(db *store.DB, mediaRoot, titleDir string) string {
	bestLen, bestView := 0, ""
	for _, r := range resolveRoots(db, "", mediaRoot) {
		root := strings.TrimRight(r.Path, "/")
		if root != "" && (titleDir == root || strings.HasPrefix(titleDir, root+"/")) && len(root) > bestLen {
			bestLen, bestView = len(root), r.ViewID
		}
	}
	return bestView
}

// titleDirFor 由条目文件路径反推“标题目录”和所属媒体库 ID。
// 约定：媒体库根下第一段目录就是标题目录（与扫描器 discoverTitles 的判据一致）。
func titleDirFor(db *store.DB, mediaRoot, itemPath string) (string, string) {
	if itemPath == "" {
		return "", ""
	}
	titleDir := scanner.TitleDir(itemPath, mediaRoot)
	if titleDir == "" || titleDir == strings.TrimRight(mediaRoot, "/") {
		return "", ""
	}
	// 归属媒体库：取路径前缀最长的那一个。
	bestLen, bestView := 0, ""
	for _, r := range resolveRoots(db, "", mediaRoot) {
		root := strings.TrimRight(r.Path, "/")
		if root != "" && (titleDir == root || strings.HasPrefix(titleDir, root+"/")) && len(root) > bestLen {
			bestLen, bestView = len(root), r.ViewID
		}
	}
	return titleDir, bestView
}

// popcornPatterns 返回网易爆米花的客户端特征；POPCORN_COMPAT=0 可整体关闭。
func popcornPatterns() []string {
	if env("POPCORN_COMPAT", "1") == "0" {
		return nil
	}
	if v := env("POPCORN_MATCH", ""); v != "" {
		var out []string
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(strings.ToLower(p)); p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	// 默认特征：客户端名/UA 里常见的关键词；不包含 emby/vidhub/infuse，避免误伤。
	// 网易爆米花在不同平台会露出不同标识：
	//   iOS/桌面：User-Agent 为 URL 编码的「网易爆米花/76 …」
	//   Android：User-Agent 为 Filmly/<版本>（它不发送 X-Emby-Client，客户端显示为 Unknown）
	return []string{"popcorn", "爆米花", "filmly", "neomagic", "netease", "163"}
}
