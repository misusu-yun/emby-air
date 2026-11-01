// Package scanner 将媒体目录树（只含 .strm）解析为 Emby 条目并批量写入索引。
//
// 布局约定（与 Emby 的“库文件夹”一致）：
//
//	<Root>/
//	  Movie Name (2025) {tmdb-273701}/           → Movie（目录内直接含 .strm）
//	  Show Name (2025) {tmdb-123}/
//	    Season 1/                                 → Season
//	      Show Name - S01E01 - Title.strm         → Episode
//	    Show Name - S01E01 - Title.strm           → Episode（无 Season 目录时归入 Specials）
//	  第1集.strm                                  → Episode
//
// ID 规则：sha1(以“/”分隔、相对 Root 的清洗后路径) 的前 32 位小写 hex，永久稳定。
// 路径清洗：去掉首尾分隔符、折叠重复分隔符、去掉 “.” 段；保留大小写（区分大小写的
// 文件系统上两者是不同文件）。
//
// 增量策略：store 未暴露读取 item_paths 的查询方法，因此本扫描器无法在本地比对
// (mtime,size)。当前的取舍是：每次全量 walk，把全部识别结果交给 UpsertItems，
// 依赖其幂等性（主键 upsert + item_paths 覆盖）来保证重复扫描不产生重复行，并通过
// 统计数与两次扫描 ID 一致性来验证。若 store 后续增加 ReadPathRecords 之类的只读方法，
// 可在 walk 阶段先载入 path→(mtime,size) 映射并跳过未变化文件，本包对外 API 不变。
package scanner

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"emby-air/internal/model"
	"emby-air/internal/store"
)

// Options 控制一次扫描。
type Options struct {
	Root       string // 媒体根目录
	BatchSize  int    // 每次 UpsertItems 的条目数，<=0 时取 500
	Workers    int    // 并发 walk 的顶层目录数，<=0 时取 4
	RatePerSec int    // >0 时限制每秒处理的目录项数
	WriteRate  int    // >0 时限制每秒写入条目数（默认 300），用于避免抢满磁盘 IO
	// Progress 每次采样回调一次（约 500ms），用于输出进度条与监控指标。
	Progress func(Progress)
	// Incremental=true 时预先载入路径索引，未变化的标题整块跳过。
	Incremental bool
	// Index 由调用方（或 Scan）载入的路径索引，Incremental=true 时生效。
	Index map[string]store.PathEntry
	// LibraryID 非空时，顶层条目（Series/Movie）的 ParentId 指向该媒体库，
	// 这样客户端按库 ID 查询才能列出内容。
	LibraryID string
	// BaseRoot 是计算条目 ID / 相对路径的固定根（默认等于 Root）。
	// 用固定根可以避免“同一文件用不同根扫描 → 生成两个不同 ID”的重复条目。
	BaseRoot string
	// RootIsTitle=true 表示 Root 本身就是一个标题目录（Pansave 定向通知），
	// 此时直接扫描 Root，而不是从子目录里发现标题。
	RootIsTitle bool
	// ForceParse=true 时定向目录即使 mtime/size 未变也重新解析并 upsert，
	// 但 Changed 计数仍按 mtime/size 判断，避免把“无变化”误报成变更。
	ForceParse bool
}

// Progress 是一次扫描的实时快照。
type Progress struct {
	Phase    string        // discover / scan / write
	Titles   int           // 已处理的标题目录数
	Total    int           // 标题目录总数
	Items    int           // 已解析条目数
	Files    int           // 已读取的 .strm 数
	Dirs     int           // 已访问目录数
	Skipped  int           // 跳过数
	Elapsed  time.Duration // 已耗时
	Rate     float64       // 条目/秒
	ETA      time.Duration // 预计剩余
	RSSBytes uint64        // 进程常驻内存
	Queue    int           // 待处理标题队列深度
}

// Result 汇总一次扫描。
type Result struct {
	Scanned   int           // 访问过的文件系统条目数
	Matched   int           // 识别出的 .strm 媒体文件数
	Indexed   int           // 识别出的条目数（含 Series/Season 容器）
	Upserted  int           // 实际提交给 store 的条目数
	WriteTime time.Duration // 写入 DB 的累计耗时
	Skipped   int           // 无法识别或刻意跳过的条目数
	Changed   int           // mtime/size 发生变化的媒体文件数（用于判断是否需要刮削）
	Series    int
	Seasons   int
	Episodes  int
	Movies    int
	Elapsed   time.Duration
	PerSecond float64 // 匹配的媒体文件数 / 秒
}

// ErrNoRoot 表示 Root 未配置或不是目录。
var ErrNoRoot = errors.New("scanner: root is empty or not a directory")

var (
	// 名字 (2025) {tmdb-273701}；年份与 provider id 均可省略。
	dirNameRe = regexp.MustCompile(`^(.*?)\s*(?:\((\d{4})\))?\s*(?:\{[a-zA-Z]+-[A-Za-z0-9]+\})?\s*$`)
	// 内置 provider id：{tmdb-1} {imdb-tt1} {tvdb-2}
	providerRe = regexp.MustCompile(`\{([a-zA-Z]+)-([A-Za-z0-9]+)\}`)
	seasonRe   = regexp.MustCompile(`(?i)^(?:season|s)\s*0*(\d{1,3})(?:\s*[-–—.]\s*.*)?$`)
	// S01E02 / s1e2 / S01.E02
	sxxexxRe = regexp.MustCompile(`(?i)s(\d{1,2})[\s._-]*e(\d{1,4})`)
	// 1x02
	nxnnRe = regexp.MustCompile(`(?i)(?:^|[^\d])(\d{1,2})x(\d{1,4})(?:[^\d]|$)`)
	// 第N集
	cnEpisodeRe = regexp.MustCompile(`第\s*(\d{1,4})\s*集`)
	// 分辨率/来源等尾巴，用于清理剧集显示名。
	releaseTailRe = regexp.MustCompile(`(?i)[\s._-]+(?:\d{3,4}p|web[-.]?dl|bluray|bdrip|hdtv|remux|x26[45]|h26[45]|hevc|aac|dts|ddp?\d).*$`)
	// 文件名去掉 SxxExx 后，剩余部分常常只剩分辨率/编码/音轨标记，需要整体丢弃。
	releaseishRe     = regexp.MustCompile(`(?i)\d{3,4}[pi]|\d+fps|fps|hdr\d*|dovi|sdr|hevc|avc|x26[45]|ddp|aac|atmos|10bit|8bit|remux|bluray`)
	leadingReleaseRe = regexp.MustCompile(`(?i)^(?:[\s._-]*(?:\d{3,4}[pi]|[\d.]+fps|web[-.]?dl|hdr\d*|dovi(?:[ .]?p\d)?|sdr|h[ .]?26[45]|x26[45]|hevc|avc|ddp[\d .]*|aac[\d .]*|atmos|[\d.]+audios?|10bit|8bit|中文字幕|国语|粤语))+`)
)

// itemSink 是扫描结果的落点。生产实现写 SQLite；测试可注入捕获实现。
type itemSink interface {
	upsert(ctx context.Context, items []model.Item, paths []store.PathRecord) error
}

// dbSink 把批量写入转发给 store.DB.UpsertItems。
type dbSink struct{ db *store.DB }

func (d dbSink) upsert(ctx context.Context, items []model.Item, paths []store.PathRecord) error {
	return d.db.UpsertItems(ctx, items, paths)
}

// DeletePath 让扫描器在增量收尾时真正删除已消失的路径（scanner 只会在扫描根范围内调用）。
// path 是相对 BaseRoot 的 item_paths 键，absPath 是 media_sources 里保存的本地绝对路径。
func (d dbSink) DeletePath(ctx context.Context, path, absPath string) error {
	_, err := d.db.DeletePath(ctx, path, absPath)
	return err
}

// Scan 遍历 opt.Root 并写入 db。可被 ctx 取消；取消时返回已完成部分的统计与 ctx.Err()。
func Scan(ctx context.Context, db *store.DB, opt Options) (Result, error) {
	// 批量写模式：一次性入库时关掉同步刷盘，结束后恢复并 checkpoint。
	if err := db.BulkBegin(ctx); err == nil {
		defer func() { _ = db.BulkEnd(context.Background()) }()
	}
	if opt.Incremental && opt.Index == nil {
		if idx, err := db.LoadPathIndex(ctx); err == nil {
			opt.Index = idx
		}
	}
	return scan(ctx, dbSink{db}, opt)
}

// scan 是 Scan 的实现，sink 可替换以便测试。
func scan(ctx context.Context, sink itemSink, opt Options) (Result, error) {
	start := time.Now()
	res := Result{}
	finish := func(err error) (Result, error) {
		res.Elapsed = time.Since(start)
		if res.Elapsed > 0 {
			res.PerSecond = float64(res.Matched) / res.Elapsed.Seconds()
		}
		return res, err
	}

	if opt.Root == "" {
		return res, ErrNoRoot
	}
	st, err := os.Stat(opt.Root)
	if err != nil || !st.IsDir() {
		if err == nil {
			return res, ErrNoRoot
		}
		return res, fmt.Errorf("%w: %v", ErrNoRoot, err)
	}
	root, err := filepath.Abs(opt.Root)
	if err != nil {
		return res, err
	}

	if opt.BatchSize <= 0 {
		opt.BatchSize = 500
	}
	if opt.Workers <= 0 {
		// 多核并发：上限 8，避免在 HDD 上把随机读打散。
		opt.Workers = runtime.NumCPU()
		if opt.Workers > 8 {
			opt.Workers = 8
		}
		if opt.Workers < 2 {
			opt.Workers = 2
		}
	}
	// 0 = 不限速（默认）。之前默认 300 条/秒，靠 time.Sleep 主动限流，
	// 这是“扫描慢、CPU 空闲”的直接原因，全量扫描时没必要。
	if opt.WriteRate < 0 {
		opt.WriteRate = 0
	}

	var limiter *time.Ticker
	if opt.RatePerSec > 0 {
		limiter = time.NewTicker(time.Second / time.Duration(opt.RatePerSec))
		defer limiter.Stop()
	}

	// 先只做「发现」：找出所有标题目录（名字里带年份或 tmdb/imdb id 的目录）。
	// 这样可以按标题并发处理，避免整库条目堆在一个 tree 里把内存撑爆。
	base := opt.BaseRoot
	if base == "" {
		base = root
	}
	base, _ = filepath.Abs(base)
	var titles []string
	if opt.RootIsTitle {
		rootAbs, absErr := filepath.Abs(root)
		if absErr != nil {
			return res, absErr
		}
		rel, relErr := filepath.Rel(base, rootAbs)
		if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return res, fmt.Errorf("scanner: root %s is outside base %s", root, base)
		}
		if rel == "." {
			return res, fmt.Errorf("scanner: RootIsTitle requires a title subdirectory")
		}
		titles = []string{filepath.ToSlash(rel)}
	} else {
		var err error
		titles, err = discoverTitles(ctx, root, base, limiter, 8)
		if err != nil {
			return res, err
		}
	}

	// 增量上下文：载入路径索引，扫描结束后删除已消失的路径。
	state := &scanState{incremental: opt.Incremental, forceParse: opt.ForceParse, index: opt.Index, libraryID: opt.LibraryID}

	// 进度采样：独立 goroutine，用原子计数读取，不干扰扫描主路径。
	var (
		doneTitles atomic.Int64
		doneItems  atomic.Int64
		doneFiles  atomic.Int64
		doneDirs   atomic.Int64
		doneSkip   atomic.Int64
	)
	progressDone := make(chan struct{})
	if opt.Progress != nil {
		go func() {
			t := time.NewTicker(500 * time.Millisecond)
			defer t.Stop()
			start := time.Now()
			for {
				select {
				case <-progressDone:
					return
				case <-t.C:
					el := time.Since(start)
					items := int(doneItems.Load())
					titlesDone := int(doneTitles.Load())
					p := Progress{
						Phase: "scan", Titles: titlesDone, Total: len(titles),
						Items: items, Files: int(doneFiles.Load()), Dirs: int(doneDirs.Load()),
						Skipped: int(doneSkip.Load()), Elapsed: el,
						RSSBytes: currentRSS(),
					}
					if el > 0 {
						p.Rate = float64(items) / el.Seconds()
					}
					if titlesDone > 0 && titlesDone < len(titles) {
						per := el / time.Duration(titlesDone)
						p.ETA = per * time.Duration(len(titles)-titlesDone)
					}
					opt.Progress(p)
				}
			}
		}()
	}
	defer close(progressDone)

	results := make(chan *tree)
	// 单条 flusher 协程串行化 UpsertItems：SQLite 只有一个写者，
	// worker 只负责解析目录，写入顺序即完成顺序（无全局顺序要求）。
	batch := newBatcher(ctx, sink, opt.BatchSize)
	written := make(chan struct {
		n         int
		err       error
		writeTime time.Duration
	}, 1)
	go func() {
		n := 0
		var err error
		var writeTime time.Duration
		writeStart := time.Now()
		for t := range results {
			if err != nil {
				continue // 出错后仍然排空，避免 worker 阻塞
			}
			start := time.Now()
			e := batch.add(t.items, t.paths)
			writeTime += time.Since(start)
			if e != nil {
				err = e
				continue
			}
			n += len(t.items)
			// 写入节流：把写入速率压到 WriteRate 条/秒以内，主动让出磁盘 IO。
			if opt.WriteRate > 0 && n > 0 {
				expected := time.Duration(float64(n) / float64(opt.WriteRate) * float64(time.Second))
				if elapsed := time.Since(writeStart); elapsed < expected {
					time.Sleep(expected - elapsed)
				}
			}
		}
		if err == nil {
			start := time.Now()
			err = batch.flush()
			writeTime += time.Since(start)
		}
		written <- struct {
			n         int
			err       error
			writeTime time.Duration
		}{n, err, writeTime}
	}()

	var mu sync.Mutex
	var c counters
	var scanErr error
	var errOnce sync.Once
	setErr := func(e error) {
		errOnce.Do(func() { scanErr = e })
	}

	// 顶层目录是并发单位：识别完全局限于单个顶层目录内，无共享状态。
	workers := opt.Workers
	if workers > len(titles) {
		workers = len(titles)
	}
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range jobs {
				if ctx.Err() != nil {
					return
				}
				t, err := scanTop(ctx, base, rel, true, limiter, state)
				doneTitles.Add(1)
				if t != nil {
					doneItems.Add(int64(len(t.items)))
					doneDirs.Add(int64(t.scanned))
					doneSkip.Add(int64(t.skipped))
					doneFiles.Add(int64(t.matched))
				}
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					setErr(err)
					return
				}
				mu.Lock()
				c.merge(t)
				mu.Unlock()
				select {
				case results <- t:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for _, rel := range titles {
		select {
		case <-ctx.Done():
			break
		case jobs <- rel:
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()

	close(results)
	w := <-written
	res.Upserted = w.n
	res.WriteTime = w.writeTime

	// 增量收尾：索引里存在、但本轮没见到的路径，说明文件已被删除。
	// 关键：只处理“本次扫描根之下”的路径；index 是全库的，直接遍历会误删其它库。
	// 只有在扫描与写库都成功时才删除，避免半途失败造成误删。
	if opt.BaseRoot != "" && state.incremental && state.index != nil && scanErr == nil && w.err == nil && ctx.Err() == nil {
		if deleter, ok := sink.(interface {
			DeletePath(context.Context, string, string) error
		}); ok {
			rootRel, relErr := filepath.Rel(base, root)
			rootRel = filepath.ToSlash(rootRel)
			scoped := relErr == nil && rootRel != ".." && !strings.HasPrefix(rootRel, "../")
			if scoped && rootRel != "." {
				for p := range state.index {
					if p != rootRel && !strings.HasPrefix(p, rootRel+"/") {
						continue
					}
					if _, seen := state.seen.Load(p); !seen {
						if err := deleter.DeletePath(ctx, p, filepath.Join(base, filepath.FromSlash(p))); err != nil {
							return finish(err)
						}
					}
				}
			} else if scoped {
				for p := range state.index {
					if _, seen := state.seen.Load(p); !seen {
						if err := deleter.DeletePath(ctx, p, filepath.Join(base, filepath.FromSlash(p))); err != nil {
							return finish(err)
						}
					}
				}
			}
		}
	}

	res.Scanned = c.scanned
	res.Matched = c.matched
	res.Indexed = c.indexed
	res.Skipped = c.skipped
	res.Changed = c.changed
	res.Series = c.series
	res.Seasons = c.seasons
	res.Episodes = c.episodes
	res.Movies = c.movies

	switch {
	case ctx.Err() != nil:
		return finish(ctx.Err())
	case scanErr != nil:
		return finish(scanErr)
	default:
		return finish(w.err)
	}
}

type counters struct {
	scanned, matched, indexed, skipped, changed int
	series, seasons, episodes, movies           int
}

func (c *counters) merge(t *tree) {
	c.scanned += t.scanned
	c.matched += t.matched
	c.skipped += t.skipped
	c.indexed += len(t.items)
	c.changed += t.changed
	for _, it := range t.items {
		switch it.Type {
		case "Series":
			c.series++
		case "Season":
			c.seasons++
		case "Episode":
			c.episodes++
		case "Movie":
			c.movies++
		}
	}
}

// tree 是单个顶层目录的扫描结果。
type tree struct {
	items   []model.Item
	paths   []store.PathRecord
	scanned int
	matched int
	skipped int
	changed int
}

func (t *tree) addItem(it model.Item) { t.items = append(t.items, it) }

// merge 合并子树的统计与结果。
func (t *tree) merge(o *tree) {
	if o == nil {
		return
	}
	t.items = append(t.items, o.items...)
	t.paths = append(t.paths, o.paths...)
	t.scanned += o.scanned
	t.matched += o.matched
	t.skipped += o.skipped
	t.changed += o.changed
}
func (t *tree) addPath(p store.PathRecord) { t.paths = append(t.paths, p) }

// scanState 是单次扫描的增量上下文。
type scanState struct {
	index       map[string]store.PathEntry
	seen        sync.Map
	incremental bool
	forceParse  bool
	libraryID   string
}

// unchanged 判断文件是否与上次扫描一致（mtime+size 都没变就跳过重建）。
func (st *scanState) unchanged(rec store.PathRecord) bool {
	if st == nil || !st.incremental || st.index == nil {
		return false
	}
	e, ok := st.index[rec.Path]
	return ok && e.MTime == rec.MTime && e.Size == rec.Size
}

type mediaFile struct {
	abs     string
	rel     string
	name    string
	rec     store.PathRecord
	version string
	changed bool
}

// currentRSS 读取进程常驻内存（Linux）。
func currentRSS() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var size, rss uint64
	if _, err := fmt.Sscanf(string(b), "%d %d", &size, &rss); err != nil {
		return 0
	}
	return rss * uint64(os.Getpagesize())
}

// findLocalImage 在目录里按候选名找图片，返回绝对路径。
func findLocalImage(dir string, names ...string) string {
	for _, n := range names {
		p := filepath.Join(dir, n)
		if fi, err := os.Stat(p); err == nil && fi.Size() > 0 {
			return p
		}
	}
	return ""
}

func localPoster(dir string) string {
	return findLocalImage(dir, "poster.jpg", "folder.jpg", "cover.jpg", "poster.png", "folder.png")
}

func localBackdrop(dir string) string {
	return findLocalImage(dir, "backdrop.jpg", "fanart.jpg", "backdrop.png", "fanart.png")
}

// IsTitleDir 判断路径名是否是标题目录（含年份或 provider id）。
func IsTitleDir(path string) bool {
	_, year, provs := parseDirName(filepath.Base(filepath.Clean(path)))
	return year > 0 || len(provs) > 0
}

// discoverTitles 遍历目录树，返回所有标题目录的相对路径。
// 判据：目录名能被 parseDirName 解析出年份或 provider id；不再向下递归，
// 因为标题目录内部是季/版本结构，交给 scanTop 处理。
func discoverTitles(ctx context.Context, root, base string, limiter *time.Ticker, maxDepth int) ([]string, error) {
	var out []string
	var walk func(abs string, depth int) error
	walk = func(abs string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			return nil // 无权限/已删除的目录直接跳过
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			// 跳过隐藏目录（含 .trash/.git/系统目录），避免无谓唤醒磁盘。
			if strings.HasPrefix(e.Name(), ".") {
				continue
			}
			if err := wait(ctx, limiter); err != nil {
				return err
			}
			subAbs := filepath.Join(abs, e.Name())
			_, year, provs := parseDirName(e.Name())
			if year > 0 || len(provs) > 0 {
				rel, rerr := filepath.Rel(base, subAbs)
				if rerr != nil {
					continue
				}
				out = append(out, filepath.ToSlash(rel))
				continue
			}
			if depth > 1 {
				if err := walk(subAbs, depth-1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root, maxDepth); err != nil {
		return nil, err
	}
	return out, nil
}

// looksLikeFlatSeries 判断“没有 Season 目录、但文件名带 SxxExx/1x02/第N集”的标题
// 应当按剧集处理，而不是因为缺少 Season 目录被误判成电影。
func looksLikeFlatSeries(direct []fs.DirEntry) bool {
	for _, e := range direct {
		if !isStrm(e.Name()) {
			continue
		}
		season, ep, _ := parseEpisodeName(e.Name(), 0)
		if season > 0 || ep > 0 {
			return true
		}
	}
	return false
}

// scanTop 识别一个标题目录：剧集按季号归并多个版本目录，电影把标题目录下
// 所有 .strm（含版本子目录）合并为一个条目，每个文件是一条 MediaSource。
func scanTop(ctx context.Context, root, top string, isDir bool, limiter *time.Ticker, st *scanState) (*tree, error) {
	t := &tree{}
	absTop := filepath.Join(root, top)

	if !isDir {
		t.scanned++
		if isStrm(top) {
			fi, err := os.Stat(absTop)
			if err != nil {
				t.skipped++
				return t, nil
			}
			name := strings.TrimSuffix(top, filepath.Ext(top))
			it := movieItem(top, name, 0, nil, absTop, fi)
			it.MediaSources = []model.MediaSource{sourceFor(top, "", absTop)}
			t.addItem(it)
			rec := record(absTop, top)
			if st != nil {
				st.seen.Store(rec.Path, struct{}{})
			}
			t.addPath(rec)
			t.matched++
		}
		return t, nil
	}

	name, year, providers := parseDirName(filepath.Base(top))
	entries, err := os.ReadDir(absTop)
	if err != nil {
		return nil, err
	}
	t.scanned++

	var direct []fs.DirEntry
	var seasonDirs, versionDirs []fs.DirEntry
	for _, e := range entries {
		if err := wait(ctx, limiter); err != nil {
			return nil, err
		}
		switch {
		case e.IsDir():
			if _, ok := parseSeasonDir(e.Name()); ok {
				seasonDirs = append(seasonDirs, e)
			} else {
				versionDirs = append(versionDirs, e)
			}
		case isStrm(e.Name()):
			direct = append(direct, e)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// ---- 剧集：同一季号的多个版本目录（Season 1 - Other / Season 1 - Dovi）归并为一季 ----
	if len(seasonDirs) > 0 || looksLikeFlatSeries(direct) {
		// 增量：先看一眼有没有文件变化，全都没变就整块跳过（不建条目、不写库）。
		if st != nil && st.incremental && !st.forceParse {
			changed := false
			for _, d := range seasonDirs {
				files, _, err := readSeasonDir(ctx, filepath.Join(absTop, d.Name()), path.Join(top, d.Name()), limiter, st)
				if err != nil {
					return nil, err
				}
				for _, f := range files {
					if f.changed {
						changed = true
						break
					}
				}
				if changed {
					break
				}
			}
			// 标题目录下直接躺着的 Specials 也必须加入 seen，并参与“是否有变化”判断；
			// 否则增量提前返回时它们会被误判为已删除。
			for _, e := range direct {
				fi, err := e.Info()
				if err != nil {
					continue
				}
				rel := path.Join(top, e.Name())
				rec := recordInfo(rel, fi)
				st.seen.Store(rec.Path, struct{}{})
				if !st.unchanged(rec) {
					changed = true
				}
			}
			if !changed {
				t.skipped += len(seasonDirs)
				return t, nil
			}
		}

		groups := map[int][]fs.DirEntry{}
		order := []int{}
		for _, d := range seasonDirs {
			num, _ := parseSeasonDir(d.Name())
			if _, ok := groups[num]; !ok {
				order = append(order, num)
			}
			groups[num] = append(groups[num], d)
		}
		sort.Ints(order)

		seriesID := ItemID(top)
		seriesPoster := localPoster(absTop)
		if seriesPoster == "" {
			for _, d := range seasonDirs {
				if p := localPoster(filepath.Join(absTop, d.Name())); p != "" {
					seriesPoster = p
					break
				}
			}
		}
		seriesBackdrop := localBackdrop(absTop)
		if seriesBackdrop == "" {
			for _, d := range seasonDirs {
				if p := localBackdrop(filepath.Join(absTop, d.Name())); p != "" {
					seriesBackdrop = p
					break
				}
			}
		}
		series := model.Item{
			Id: seriesID, Name: name, Type: "Series", ProductionYear: year, ParentId: st.libraryID,
			ImageRef: seriesPoster, BackdropRef: seriesBackdrop,
			IsFolder: true, Path: absTop, SortName: name, ProviderIds: providers, DateCreated: now,
		}
		t.addItem(series)

		for _, num := range order {
			seasonRel := path.Join(top, fmt.Sprintf("Season %d", num))
			seasonID := ItemID(seasonRel)
			seasonAbsDir := filepath.Join(absTop, fmt.Sprintf("Season %d", num))
			seasonPoster := localPoster(seasonAbsDir)
			if seasonPoster == "" {
				for _, d := range groups[num] {
					if p := localPoster(filepath.Join(absTop, d.Name())); p != "" {
						seasonPoster = p
						break
					}
				}
			}
			t.addItem(model.Item{
				Id: seasonID, Name: fmt.Sprintf("Season %d", num), Type: "Season",
				ImageRef: seasonPoster,
				ParentId: seriesID, SeriesId: seriesID, SeriesName: name,
				ParentIndexNumber: num, IndexNumber: num, IsFolder: true,
				Path:     filepath.Join(absTop, fmt.Sprintf("Season %d", num)),
				SortName: fmt.Sprintf("%04d", num), DateCreated: now,
			})

			eps := map[int][]mediaFile{}
			for _, d := range groups[num] {
				seasonAbs := filepath.Join(absTop, d.Name())
				seasonRelDir := path.Join(top, d.Name())
				files, skipped, err := readSeasonDir(ctx, seasonAbs, seasonRelDir, limiter, st)
				if err != nil {
					return nil, err
				}
				t.scanned += skipped + len(files)
				t.skipped += skipped
				version := versionLabel(d.Name())
				for _, f := range files {
					// 季目录内以目录季号为准，忽略文件名里的季号（避免 S02 文件放进 Season 1 时错位）。
					_, epNum, _ := parseEpisodeName(f.name, num)
					f.version = version
					eps[epNum] = append(eps[epNum], f)
				}
			}

			nums := make([]int, 0, len(eps))
			for k := range eps {
				nums = append(nums, k)
			}
			sort.Ints(nums)
			for _, en := range nums {
				fs := eps[en]
				if len(fs) == 0 {
					continue
				}
				_, _, epName := parseEpisodeName(fs[0].name, num)
				if strings.TrimSpace(epName) == "" {
					epName = fmt.Sprintf("Episode %d", en)
				}
				epRel := path.Join(top, fmt.Sprintf("S%02dE%03d", num, en))
				ep := model.Item{
					Id: ItemID(epRel), Name: epName, Type: "Episode",
					ParentId: seasonID, SeriesId: seriesID, SeriesName: name, SeasonId: seasonID,
					ParentIndexNumber: num, IndexNumber: en, ProductionYear: year,
					Path: fs[0].abs, Container: "strm",
					SortName: fmt.Sprintf("%04d-%04d", num, en), DateCreated: now,
				}
				for _, f := range fs {
					ep.MediaSources = append(ep.MediaSources, sourceFor(f.rel, f.version, f.abs))
					rec := f.rec
					rec.ItemId = ep.Id
					t.addPath(rec)
				}
				t.addItem(ep)
				t.matched++
			}
		}
		// 标题目录下直接躺着的 .strm：
		// - 没有 Season 目录（平铺剧集）：按文件名 SxxExx 的季号归入对应 Season；
		// - 已有 Season 目录：这些文件通常是 Specials，归入 Season 0。
		if len(direct) > 0 {
			if len(seasonDirs) == 0 {
				groups := map[int][]mediaFile{}
				for _, e := range direct {
					absFile := filepath.Join(absTop, e.Name())
					rel := path.Join(top, e.Name())
					fi, err := os.Stat(absFile)
					if err != nil {
						t.skipped++
						continue
					}
					season, _, _ := parseEpisodeName(e.Name(), 0)
					f := mediaFile{abs: absFile, rel: rel, name: e.Name(), rec: recordInfo(rel, fi), changed: st == nil || !st.unchanged(recordInfo(rel, fi))}
					if st != nil {
						st.seen.Store(f.rec.Path, struct{}{})
					}
					if f.changed {
						t.changed++
					}
					groups[season] = append(groups[season], f)
				}
				seasons := make([]int, 0, len(groups))
				for sn := range groups {
					seasons = append(seasons, sn)
				}
				sort.Ints(seasons)
				for _, sn := range seasons {
					seasonRel := path.Join(top, fmt.Sprintf("Season %d", sn))
					seasonName := fmt.Sprintf("Season %d", sn)
					sortName := fmt.Sprintf("%04d", sn)
					if sn == 0 {
						seasonRel = path.Join(top, "Specials")
						seasonName = "Specials"
						sortName = "0000"
					}
					seasonID := ItemID(seasonRel)
					t.addItem(model.Item{
						Id: seasonID, Name: seasonName, Type: "Season",
						ParentId: seriesID, SeriesId: seriesID, SeriesName: name,
						ParentIndexNumber: sn, IndexNumber: sn, IsFolder: true,
						Path: absTop, SortName: sortName, DateCreated: now,
					})
					eps := map[int][]mediaFile{}
					for _, f := range groups[sn] {
						_, epNum, _ := parseEpisodeName(f.name, sn)
						eps[epNum] = append(eps[epNum], f)
					}
					epNums := make([]int, 0, len(eps))
					for en := range eps {
						epNums = append(epNums, en)
					}
					sort.Ints(epNums)
					for _, en := range epNums {
						fs := eps[en]
						if len(fs) == 0 {
							continue
						}
						for _, f := range fs {
							if f.changed {
								t.changed++
							}
						}
						_, _, epName := parseEpisodeName(fs[0].name, sn)
						ep := episodeItem(fs[0], epName, en, sn, series, seasonID, year, now)
						ep.MediaSources = nil
						for _, f := range fs {
							ep.MediaSources = append(ep.MediaSources, sourceFor(f.rel, "", f.abs))
							rec := f.rec
							rec.ItemId = ep.Id
							t.addPath(rec)
						}
						t.addItem(ep)
						t.matched++
					}
				}
			} else {
				// 有 Season 目录时，direct 文件保持 Specials 语义。
				seasonRel := path.Join(top, "Specials")
				seasonID := ItemID(seasonRel)
				t.addItem(model.Item{
					Id: seasonID, Name: "Specials", Type: "Season",
					ParentId: seriesID, SeriesId: seriesID, SeriesName: name,
					ParentIndexNumber: 0, IndexNumber: 0, IsFolder: true,
					Path: absTop, SortName: "0000", DateCreated: now,
				})
				for _, e := range direct {
					absFile := filepath.Join(absTop, e.Name())
					rel := path.Join(top, e.Name())
					fi, err := os.Stat(absFile)
					if err != nil {
						t.skipped++
						continue
					}
					_, epNum, epName := parseEpisodeName(e.Name(), 0)
					f := mediaFile{abs: absFile, rel: rel, name: e.Name(), rec: recordInfo(rel, fi), changed: st == nil || !st.unchanged(recordInfo(rel, fi))}
					if st != nil {
						st.seen.Store(f.rec.Path, struct{}{})
					}
					if f.changed {
						t.changed++
					}
					ep := episodeItem(f, epName, epNum, 0, series, seasonID, year, now)
					ep.MediaSources = []model.MediaSource{sourceFor(rel, "", absFile)}
					t.addItem(ep)
					rec := f.rec
					rec.ItemId = ep.Id
					t.addPath(rec)
					t.matched++
				}
			}
		}
		return t, nil
	}

	// ---- 电影：标题目录下的所有 strm（含版本子目录）合并为一个条目 ----
	files := make([]mediaFile, 0, len(direct))
	for _, e := range direct {
		absFile := filepath.Join(absTop, e.Name())
		rel := path.Join(top, e.Name())
		rec := record(absFile, rel)
		if st != nil {
			st.seen.Store(rec.Path, struct{}{})
		}
		changed := st == nil || !st.unchanged(rec)
		files = append(files, mediaFile{abs: absFile, rel: rel, name: e.Name(), rec: rec, changed: changed})
	}
	for _, d := range versionDirs {
		subAbs := filepath.Join(absTop, d.Name())
		subRel := path.Join(top, d.Name())
		got, skipped, err := readSeasonDir(ctx, subAbs, subRel, limiter, st)
		if err != nil {
			return nil, err
		}
		t.scanned += skipped + len(got)
		t.skipped += skipped
		version := versionLabel(d.Name())
		for _, f := range got {
			f.version = version
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		t.skipped += len(versionDirs)
		return t, nil
	}
	for _, f := range files {
		if f.changed {
			t.changed++
		}
	}
	if st != nil && st.incremental && !st.forceParse {
		changed := false
		for _, f := range files {
			if f.changed {
				changed = true
				break
			}
		}
		if !changed {
			t.skipped++
			return t, nil
		}
	}
	fi, err := os.Stat(files[0].abs)
	if err != nil {
		return nil, err
	}
	movie := movieItem(top, name, year, providers, files[0].abs, fi)
	movie.ParentId = st.libraryID
	movie.ImageRef = localPoster(absTop)
	movie.BackdropRef = localBackdrop(absTop)
	if movie.ImageRef == "" {
		for _, d := range versionDirs {
			if p := localPoster(filepath.Join(absTop, d.Name())); p != "" {
				movie.ImageRef = p
				break
			}
		}
	}
	if movie.BackdropRef == "" {
		for _, d := range versionDirs {
			if p := localBackdrop(filepath.Join(absTop, d.Name())); p != "" {
				movie.BackdropRef = p
				break
			}
		}
	}
	movie.MediaSources = nil
	for _, f := range files {
		movie.MediaSources = append(movie.MediaSources, sourceFor(f.rel, f.version, f.abs))
		rec := f.rec
		rec.ItemId = movie.Id
		t.addPath(rec)
	}
	t.addItem(movie)
	t.matched++
	return t, nil
}

// versionLabel 从 "Season 1 - Dovi" / "标题 - Other" 里取版本名（"Dovi" / "Other"）。
func versionLabel(dir string) string {
	if i := strings.LastIndex(dir, " - "); i >= 0 {
		return strings.TrimSpace(dir[i+3:])
	}
	return strings.TrimSpace(dir)
}

// sourceFor 构造一条 MediaSource；label 为空时用 "默认"。
func sourceFor(rel, label, abs string) model.MediaSource {
	if better := mediaLabel(filepath.Base(abs)); better != "" {
		label = better
	} else if label == "" {
		label = "默认"
	}
	return model.MediaSource{
		Id: ItemID(rel), Name: label, Path: abs, Container: "strm",
		SupportsDirectPlay: true, SupportsDirectStream: true, SupportsTranscoding: false,
	}
}

func movieItem(fileRel, name string, year int, providers map[string]string, abs string, fi fs.FileInfo) model.Item {
	return model.Item{
		Id:             ItemID(fileRel),
		Name:           name,
		Type:           "Movie",
		ProductionYear: year,
		IsFolder:       false,
		Path:           abs,
		SortName:       name,
		ProviderIds:    providers,
		Container:      "strm",
		MediaSources:   []model.MediaSource{mediaSource(fileRel)},
		DateCreated:    fi.ModTime().UTC().Format(time.RFC3339),
	}
}

func episodeItem(f mediaFile, name string, epNum, seasonNum int, series model.Item, seasonID string, year int, created string) model.Item {
	return model.Item{
		Id:                ItemID(f.rel),
		Name:              name,
		Type:              "Episode",
		ParentId:          seasonID,
		SeriesId:          series.Id,
		SeriesName:        series.Name,
		SeasonId:          seasonID,
		ParentIndexNumber: seasonNum,
		IndexNumber:       epNum,
		ProductionYear:    year,
		IsFolder:          false,
		Path:              f.abs,
		SortName:          fmt.Sprintf("%04d-%04d", seasonNum, epNum),
		Container:         "strm",
		MediaSources:      []model.MediaSource{mediaSource(f.rel)},
		DateCreated:       created,
	}
}

// readSeasonDir 返回季目录下的 .strm 文件（按名排序）与跳过的条目数。
func readSeasonDir(ctx context.Context, abs, rel string, limiter *time.Ticker, st *scanState) ([]mediaFile, int, error) {
	entries, err := os.ReadDir(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	var out []mediaFile
	skipped := 1 // 目录自身
	for _, e := range entries {
		if err := wait(ctx, limiter); err != nil {
			return nil, 0, err
		}
		if e.IsDir() || !isStrm(e.Name()) {
			skipped++
			continue
		}
		absFile := filepath.Join(abs, e.Name())
		fi, err := e.Info()
		if err != nil {
			skipped++
			continue
		}
		fileRel := path.Join(rel, e.Name())
		rec := recordInfo(fileRel, fi)
		if st != nil {
			st.seen.Store(rec.Path, struct{}{})
		}
		out = append(out, mediaFile{abs: absFile, rel: fileRel, name: e.Name(), rec: rec, changed: !st.unchanged(rec)})
	}
	return out, skipped, nil
}

// wait 在限速时阻塞，并尊重 ctx。
func wait(ctx context.Context, limiter *time.Ticker) error {
	if limiter == nil {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-limiter.C:
		return nil
	}
}

func record(abs, rel string) store.PathRecord {
	r := store.PathRecord{Path: rel, ItemId: ItemID(rel)}
	if fi, err := os.Stat(abs); err == nil {
		r.MTime = fi.ModTime().Unix()
		r.Size = fi.Size()
	}
	return r
}

// recordInfo 用 dirent 已经拿到（或一次 stat 取得）的 FileInfo 构造记录，
// 避免同一文件被 stat 两次——扫描 26 万文件时这是成倍的 syscall 开销。
func recordInfo(rel string, fi fs.FileInfo) store.PathRecord {
	r := store.PathRecord{Path: rel, ItemId: ItemID(rel)}
	if fi != nil {
		r.MTime = fi.ModTime().Unix()
		r.Size = fi.Size()
	}
	return r
}

// mediaSource 只用相对路径构造 MediaSource；真实直链在播放时读取 STRM 内容。
func mediaSource(rel string) model.MediaSource {
	return model.MediaSource{
		Id:                   ItemID(rel),
		Name:                 "strm",
		Container:            "strm",
		SupportsDirectPlay:   true,
		SupportsDirectStream: true,
		SupportsTranscoding:  false,
	}
}

// hasLetter 判断是否还有实质文字（排除只剩数字/标点的情况）。
func hasLetter(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

func isStrm(name string) bool { return strings.EqualFold(filepath.Ext(name), ".strm") }

func sortEntries(in []fs.DirEntry) {
	sort.Slice(in, func(i, j int) bool { return in[i].Name() < in[j].Name() })
}

// parseDirName 解析 “名字 (2025) {tmdb-273701}”。
func parseDirName(base string) (name string, year int, providers map[string]string) {
	name = strings.TrimSpace(base)
	for _, m := range providerRe.FindAllStringSubmatch(base, -1) {
		if providers == nil {
			providers = map[string]string{}
		}
		providers[strings.ToLower(m[1])] = m[2]
	}
	clean := providerRe.ReplaceAllString(base, "")
	if m := dirNameRe.FindStringSubmatch(strings.TrimSpace(clean)); m != nil {
		if m[2] != "" {
			year, _ = strconv.Atoi(m[2])
		}
		if n := strings.TrimSpace(m[1]); n != "" {
			name = n
		}
	}
	if name == "" {
		name = base
	}
	return name, year, providers
}

// parseSeasonDir 识别 “Season 1” / “S01”。
func parseSeasonDir(base string) (int, bool) {
	m := seasonRe.FindStringSubmatch(strings.TrimSpace(base))
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return n, true
}

// parseEpisodeName 从文件名提取集号与显示名。
// 优先级：SxxExx → 1x02 → 第N集；都不匹配时集号为 0、显示名取完整 stem。
func parseEpisodeName(base string, defaultSeason int) (int, int, string) {
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	season, ep := defaultSeason, 0

	var idx []int
	if m := sxxexxRe.FindStringSubmatchIndex(stem); m != nil {
		if s, err := strconv.Atoi(stem[m[2]:m[3]]); err == nil {
			season = s
		}
		ep, _ = strconv.Atoi(stem[m[4]:m[5]])
		idx = []int{m[0], m[1]}
	} else if m := nxnnRe.FindStringSubmatchIndex(stem); m != nil {
		if s, err := strconv.Atoi(stem[m[2]:m[3]]); err == nil {
			season = s
		}
		ep, _ = strconv.Atoi(stem[m[4]:m[5]])
		idx = []int{m[2], m[3]}
	} else if m := cnEpisodeRe.FindStringSubmatchIndex(stem); m != nil {
		ep, _ = strconv.Atoi(stem[m[2]:m[3]])
		idx = []int{m[0], m[1]}
	}
	if season < 0 {
		season = 0
	}

	name := stem
	if idx != nil {
		if tail := strings.TrimLeft(stem[idx[1]:], " ._-"); tail != "" {
			name = tail
		}
	}
	name = strings.TrimSpace(releaseTailRe.ReplaceAllString(name, ""))
	name = strings.TrimSpace(leadingReleaseRe.ReplaceAllString(name, ""))
	name = strings.TrimLeft(name, " ._-")
	// 没有 " - 标题" 分隔、只剩发行标记（如 ".2160p.30fps.MyVideo.WEB-DL"）时，
	// 视为该集没有内嵌标题，交给 TMDB 或退回「第 N 集」。
	if strings.Contains(strings.ToLower(name), "web") || releaseishRe.MatchString(name) || !hasLetter(name) {
		if ep > 0 {
			name = fmt.Sprintf("第 %d 集", ep)
		} else {
			name = stem
		}
	}
	return season, ep, name
}

// ItemID 返回 sha1(清洗后路径) 前 32 位小写 hex。
// 纯函数：同一相对路径永远得到同一 ID（contract 规则 6）。
func ItemID(rel string) string {
	sum := sha1.Sum([]byte(cleanPath(rel)))
	return hex.EncodeToString(sum[:])[:32]
}

// cleanPath 归一化路径用于 ID 计算。
func cleanPath(p string) string {
	p = filepath.ToSlash(p)
	parts := strings.Split(p, "/")
	out := parts[:0]
	for _, seg := range parts {
		seg = strings.TrimSpace(seg)
		if seg == "" || seg == "." {
			continue
		}
		out = append(out, seg)
	}
	return strings.Join(out, "/")
}

// batcher 串行化下游写入：SQLite 单写者，且批次边界可预期。
// 由于只有 flusher 协程调用 add/commit，缓冲无需加锁。
type batcher struct {
	ctx   context.Context
	sink  itemSink
	size  int
	buf   []model.Item
	paths []store.PathRecord
}

func newBatcher(ctx context.Context, sink itemSink, size int) *batcher {
	return &batcher{ctx: ctx, sink: sink, size: size}
}

func (b *batcher) add(items []model.Item, paths []store.PathRecord) error {
	b.buf = append(b.buf, items...)
	b.paths = append(b.paths, paths...)
	for len(b.buf) >= b.size {
		if err := b.ctx.Err(); err != nil {
			return err
		}
		if err := b.commit(b.buf[:b.size]); err != nil {
			return err
		}
		b.buf = append(b.buf[:0], b.buf[b.size:]...)
	}
	return nil
}

func (b *batcher) flush() error {
	if len(b.buf) == 0 && len(b.paths) == 0 {
		return nil
	}
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if err := b.commit(b.buf); err != nil {
		return err
	}
	b.buf = b.buf[:0]
	return nil
}

// commit 提交一批条目。path 记录与条目并非一一对应（Series/Season 没有 path），
// 因此每次提交都带上当批累积的全部 path，并在提交后清空。
// commit 带 BUSY 重试：扫描期间进度上报/HTTP 写入会短暂抢写锁，
// 无限速后冲突更频繁，单次失败不该让整轮扫描报废。
func (b *batcher) commitRetry(items []model.Item, paths []store.PathRecord) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = b.sink.upsert(b.ctx, items, paths)
		if err == nil || !isBusyErr(err) {
			return err
		}
		time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
	return err
}

func isBusyErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "sqlite_busy")
}

func (b *batcher) commit(items []model.Item) error {
	its := make([]model.Item, len(items))
	copy(its, items)
	ps := make([]store.PathRecord, len(b.paths))
	copy(ps, b.paths)

	if err := b.commitRetry(its, ps); err != nil {
		return err
	}
	b.paths = b.paths[:0]
	return nil
}

// mediaLabel 从 strm 文件名生成可读的版本名，例如 "4K · 杜比视界 · 国语"。
func mediaLabel(fileName string) string {
	stem := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	if dec, err := url.PathUnescape(stem); err == nil {
		stem = dec
	}
	lower := strings.ToLower(stem)
	var parts []string
	switch {
	case strings.Contains(lower, "2160p") || strings.Contains(lower, "4k"):
		parts = append(parts, "4K")
	case strings.Contains(lower, "1080p"):
		parts = append(parts, "1080P")
	case strings.Contains(lower, "720p"):
		parts = append(parts, "720P")
	}
	switch {
	case strings.Contains(lower, "dovi") || strings.Contains(lower, "dolby vision") || strings.Contains(lower, "dv"):
		parts = append(parts, "杜比视界")
	case strings.Contains(lower, "hdr10+") || strings.Contains(lower, "hdr10"):
		parts = append(parts, "HDR10")
	case strings.Contains(lower, "hdr"):
		parts = append(parts, "HDR")
	case strings.Contains(lower, "sdr"):
		parts = append(parts, "SDR")
	}
	if strings.Contains(lower, "atmos") || strings.Contains(lower, "全景声") {
		parts = append(parts, "全景声")
	}
	if strings.Contains(stem, "国语") {
		parts = append(parts, "国语")
	}
	if strings.Contains(stem, "粤语") {
		parts = append(parts, "粤语")
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " · ")
}

// TitleDir 由媒体文件路径反推“标题目录”。
// 规则：从文件所在目录向上爬，只要父目录名能被 parseDirName 识别为标题（含年份或 provider id）
// 就继续上移，最终停在最高的那个标题目录；不会越过媒体根。
// 例：
//
//	/test/华语电影/一念 (2019) {tmdb-1}/一念 - Other/a.strm → .../一念 (2019) {tmdb-1}
//	/test/华语剧集/某剧 (2024) {tmdb-2}（路径本身是目录）        → 该目录
func TitleDir(path, root string) string {
	if path == "" {
		return ""
	}
	if r, err := filepath.Abs(root); err == nil {
		root = strings.TrimRight(r, string(os.PathSeparator))
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	dir := abs
	if fi, err := os.Stat(abs); err == nil {
		if !fi.IsDir() {
			dir = filepath.Dir(abs)
		}
	} else if filepath.Ext(abs) != "" {
		// 文件已不存在（例如刚被删除）时，用扩展名判断它本来是文件。
		dir = filepath.Dir(abs)
	}

	// 向上最多找 3 层：版本目录 → 标题目录 → 分类目录。
	// 取“最接近媒体根”的那个标题目录（含年份或 {tmdb-*}），这样多版本目录不会变成删除目标。
	best := ""
	cur := dir
	for i := 0; i < 3; i++ {
		if cur == root || !strings.HasPrefix(cur, root+string(os.PathSeparator)) {
			break
		}
		_, year, provs := parseDirName(filepath.Base(cur))
		if year > 0 || len(provs) > 0 {
			best = cur
		}
		cur = filepath.Dir(cur)
	}
	if best == "" {
		return dir
	}
	return best
}
