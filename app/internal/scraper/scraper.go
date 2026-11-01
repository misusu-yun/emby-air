// Package scraper 把扫描结果补齐成可展示的元数据：NFO 优先，TMDB 兜底。
//
// 流程（每个条目）：
//  1. 从目录名/文件名取标题、年份、{tmdb-<id>} 提示。
//  2. 读同目录 NFO（movie.nfo / tvshow.nfo / <stem>.nfo / Season n/season.nfo）。
//     有 NFO 且带唯一 ID 时直接使用，不再请求 TMDB（除非 force）。
//  3. TMDB：优先 ID 直连；否则搜索（标题 + 年份），只用“足够像”的命中。
//  4. 图片：海报/背景图下载到 <datadir>/cache/images/<server>/<itemid>.jpg，
//     并写回 image_ref / backdrop_ref。
//
// 断点续跑：每个条目的结果写进 scrape_state；失败 attempt+1，
// 达到 MaxAttempts 后不再重试（失败隔离），可用 -force 重置。
package scraper

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"emby-air/internal/model"
	"emby-air/internal/nfo"
	"emby-air/internal/store"
	"emby-air/internal/tmdb"
)

// Store 是 scraper 需要的存储接口（便于测试注入）。
type Store interface {
	ListScrapeCandidates(ctx context.Context, limit int, force bool, paths []string, kinds []string, afterId string, maxAttempts int) ([]store.ScrapeCandidate, error)
	ScrapeCandidateByID(ctx context.Context, id string) (*store.ScrapeCandidate, error)
	ItemPath(ctx context.Context, id string) string
	SeasonNumbersOf(ctx context.Context, seriesID string) ([]int, error)
	EpisodeIndex(ctx context.Context, seriesID string) (map[[2]int]string, error)
	ApplyMeta(ctx context.Context, p store.MetaPatch, m store.ScrapeMark) error
	PendingScrapes(ctx context.Context) (int, error)
}

// Options 控制一次刮削。
type Options struct {
	Root        string  // MediaRoot：NFO/本地图片都在其下
	ImageDir    string  // 图片缓存根目录
	ServerID    string  // 图片 URL 里的 server id
	Force       bool    // 忽略 scrape_state，全部重刮
	ForceImages bool    // 强制重下海报/背景（不重复拉详情缓存，切到新的选图规则时用）
	MaxAge      int     // 天：超过之后重新刮（0 取默认 30）
	MaxAttempts int     // scrape_state.attempts 达到该值后跳过（0 取 5）
	Batch       int     // 每次从库取多少条目
	Workers     int     // 并发刮削数（受 TMDB 速率限制约束）
	RatePerSec  float64 // TMDB 请求速率上限（0 取 4）
	Logger      *log.Logger
	Limit       int // >0 时处理到该条数即停（CLI 的 -limit）
	// MaxConsecutiveFailures 连续失败达到该值就停止整轮刮削（防止网络/密钥问题空转）。
	MaxConsecutiveFailures int
	// LibraryPaths 非空时只刮这些根路径下的条目（一个库可有多路径）。
	LibraryPaths []string
	// Kinds 限制刮削的条目类型：剧集库为 ["Series"]，电影库为 ["Movie"]；空表示两者都刮。
	Kinds []string
	// UseNFO 启用 NFO sidecar 读取与写盘。默认关闭：本系统以数据库为主，
	// NFO 不参与元数据来源，也不在刮削后写盘。
	UseNFO bool
	// OnItem 即将处理某条目时回调（用于任务中心展示“正在刮削：xxx”）。
	OnItem func(name string)
	// OnProgress 每条处理完回调（done 为已处理数）。
	OnProgress func(done int)
}

// Result 汇总一次刮削。
type Result struct {
	Processed int
	FromNFO   int
	FromTMDB  int
	NotFound  int
	Failed    int
	Skipped   int
	Images    int
	Elapsed   time.Duration
}

// Scraper 持有依赖。
type Scraper struct {
	db   Store
	tmdb *tmdb.Client
	opt  Options
}

// New 构造 Scraper。
func New(db Store, client *tmdb.Client, opt Options) *Scraper {
	// TMDB 官方建议 ≤50 req/s，但实测 15 req/s 以上容易被风控，这里做硬上限。
	if opt.RatePerSec <= 0 {
		opt.RatePerSec = 15
	}
	if opt.RatePerSec > 15 {
		if opt.Logger != nil {
			opt.Logger.Printf("刮削限速：请求速率 %.1f/s 超过安全上限，已限制为 15/s", opt.RatePerSec)
		}
		opt.RatePerSec = 15
	}
	if opt.MaxConsecutiveFailures <= 0 {
		opt.MaxConsecutiveFailures = 10
	}
	if opt.Batch <= 0 {
		opt.Batch = 200
	}
	if opt.Workers <= 0 {
		opt.Workers = 2
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = 5
	}
	if opt.Logger == nil {
		opt.Logger = log.Default()
	}
	// 限速下沉到 TMDB 客户端：详情、季集、图片下载都经过同一个令牌桶，
	// 避免“每条限速”但一条内并发打多个请求导致超速。
	if client != nil {
		if opt.Force {
			client = client.WithForceRefresh()
		} else if opt.ForceImages {
			client = client.WithForceImages()
		}
		client = client.WithRate(opt.RatePerSec)
	}
	return &Scraper{db: db, tmdb: client, opt: opt}
}

// WithLimit 返回一个处理到 limit 条即停的副本（CLI 的 -limit）。
func (s *Scraper) WithLimit(limit int) *Scraper {
	cp := *s
	cp.opt.Limit = limit
	return &cp
}

// Run 刮削直到候选耗尽。ctx 取消时返回已完成统计。
func (s *Scraper) Run(ctx context.Context) (res Result, err error) {
	start := time.Now()
	defer func() { res.Elapsed = time.Since(start) }()
	// 派生可取消的 ctx：提前返回（TMDB 鉴权错误/连续失败）时先 cancel 再排空 out，
	// 保证 Run 返回时 worker 与 producer 都已退出、不会再写库。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// done 是兜底去重：即使 Store 忽略 afterId 游标（如测试替身），
	// 同一轮也不会重复刮削同一条目。真正的遍历由 id 游标保证。
	done := make(map[string]bool)
	var lastId string

	// TMDB 速率上限：自我约束 ≤15 req/s（官方软上限约 40，留足风控余量）。
	rate := s.opt.RatePerSec
	if rate <= 0 {
		rate = 4
	}
	if rate > 15 {
		rate = 15
	}
	ticker := time.NewTicker(rl(rate))
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		prevLast := lastId
		raw, err := s.db.ListScrapeCandidates(ctx, s.opt.Batch, s.opt.Force, s.opt.LibraryPaths, s.opt.Kinds, lastId, s.opt.MaxAttempts)
		if err != nil {
			return res, err
		}
		// 游标按本批最大 id 前进：无论本批处理了多少，下一轮都不会再取到同一批，
		// 保证 force（忽略 scrape_state）也能遍历全部候选而不是只跑一个 Batch。
		for _, c := range raw {
			if c.Id > lastId {
				lastId = c.Id
			}
		}
		cands := make([]store.ScrapeCandidate, 0, len(raw))
		for _, c := range raw {
			if done[c.Id] || c.Id <= prevLast {
				continue
			}
			cands = append(cands, c)
		}
		if len(cands) == 0 {
			break
		}
		if s.opt.Limit > 0 {
			if left := s.opt.Limit - res.Processed - res.Skipped; left <= 0 {
				break
			} else if left < len(cands) {
				cands = cands[:left]
			}
		}
		// attempts 达到上限的条目已在 SQL（scrapeWhere）里过滤，Count 与 List 同口径，
		// 不需要在内存里二次过滤（否则 Skipped 重复累加、窗口被占满时进度卡在 0）。
		batch := cands

		jobs := make(chan store.ScrapeCandidate)
		out := make(chan itemResult, len(batch))
		var wg sync.WaitGroup
		for range s.opt.Workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for c := range jobs {
					// 速率限制放在请求前：下载图片也算一次请求量。
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
					if s.opt.OnItem != nil {
						s.opt.OnItem(c.Name)
					}
					out <- s.scrapeOne(ctx, c)
				}
			}()
		}
		go func() {
			for _, c := range batch {
				select {
				case <-ctx.Done():
					close(jobs)
					return
				case jobs <- c:
				}
			}
			close(jobs)
		}()
		// 边完成边上报：不再等整批 200 条处理完才更新进度。
		go func() {
			wg.Wait()
			close(out)
		}()

		consecutiveFailures := 0
		var abortErr error
	collect:
		for r := range out {
			done[r.id] = true // 同一轮内绝不重复处理同一条目
			res.Processed++
			res.Images += r.images
			if s.opt.OnProgress != nil {
				s.opt.OnProgress(res.Processed)
			}
			label := fmt.Sprintf("[%s] %s", r.kind, r.name)
			switch {
			case r.err != nil && errors.Is(r.err, ctx.Err()):
				// 取消导致的失败不刷屏
			case r.err != nil:
				res.Failed++
				consecutiveFailures++
				s.opt.Logger.Printf("刮削失败 %s：%v", label, r.err)
				if isFatalTMDBError(r.err) {
					s.opt.Logger.Printf("刮削中止：TMDB 返回鉴权/权限错误，请检查 API Key（%v）", r.err)
					abortErr = r.err
					break collect
				}
				if consecutiveFailures >= s.opt.MaxConsecutiveFailures {
					abortErr = fmt.Errorf("连续 %d 条刮削失败，已自动停止（最后错误：%v）", consecutiveFailures, r.err)
					s.opt.Logger.Printf("%v", abortErr)
					break collect
				}
			case r.applyErr != nil:
				// 元数据抓到了但没写进库（条目可能已被删除/ID 变更）：不能算成功。
				res.Failed++
				consecutiveFailures++
				s.opt.Logger.Printf("刮削写库失败 %s：%v", label, r.applyErr)
			case r.nfo && !r.found:
				res.FromNFO++
				s.opt.Logger.Printf("刮削成功 %s ← NFO（本地图片 %d）", label, r.images)
			case r.found:
				res.FromTMDB++
				s.opt.Logger.Printf("刮削成功 %s ← TMDB（图片 %d）", label, r.images)
			default:
				res.NotFound++
				s.opt.Logger.Printf("刮削未找到 %s（TMDB 无匹配）", label)
			}
			if r.err == nil && r.applyErr == nil {
				consecutiveFailures = 0
			}
		}
		if abortErr != nil {
			// 先取消：worker 不会再用 ctx 写库；producer 也会停下。
			// 然后把 out 读干直到关闭，确保没有 goroutine 残留。
			cancel()
			for range out {
			}
			return res, abortErr
		}
		if res.Processed%1000 < len(batch) {
			s.opt.Logger.Printf("刮削进度：已处理 %d（NFO %d / TMDB %d / 未找到 %d / 失败 %d），耗时 %s",
				res.Processed, res.FromNFO, res.FromTMDB, res.NotFound, res.Failed,
				time.Since(start).Round(time.Second))
		}
	}
	res.Elapsed = time.Since(start)
	s.opt.Logger.Printf("刮削结束：共 %d（NFO %d / TMDB %d / 未找到 %d / 失败 %d / 跳过 %d），图片 %d 张，耗时 %s",
		res.Processed, res.FromNFO, res.FromTMDB, res.NotFound, res.Failed, res.Skipped, res.Images,
		res.Elapsed.Round(time.Second))
	return res, nil
}

type itemResult struct {
	id       string
	name     string
	kind     string
	nfo      bool
	found    bool
	images   int
	err      error
	applyErr error
}

// ---- 单个条目 ----

func (s *Scraper) scrapeOne(ctx context.Context, c store.ScrapeCandidate) itemResult {
	r := itemResult{id: c.Id, name: c.Name, kind: c.Type}
	patch := store.MetaPatch{Id: c.Id} // 必须带 Id：ApplyMeta 靠它定位条目
	var ok bool
	if s.opt.UseNFO {
		patch, _, ok = s.local(c)
	}
	r.nfo = ok
	var mark store.ScrapeMark

	// 仅当条目没有 NFO、或 NFO 缺图时才请求 TMDB；无密钥时直接按 NFO/本地图落库。
	episodesDone := false
	if s.tmdb.Enabled() && (!ok || patch.ImageRef == "" || patch.BackdropRef == "") {
		episodesDone = c.Type == "Series" // fillTMDB 的 Series 分支内部会回填分集
		// NFO 已给出的字段不交给 TMDB 覆盖（NFO 优先），故先留一份基线。
		base := patch
		found, err := s.fillTMDB(ctx, c, &patch, &r)
		if r.nfo {
			patch = keepNFOGaps(base, patch)
		}
		switch {
		case err != nil:
			r.err = err
			mark = store.ScrapeMark{ItemId: c.Id, Status: "failed", Source: "tmdb", Error: truncate(err.Error(), 300)}
			_ = s.db.ApplyMeta(ctx, store.MetaPatch{Id: c.Id}, mark)
			return r
		case !found:
			r.err = nil
			mark = store.ScrapeMark{ItemId: c.Id, Status: "notfound", Source: "tmdb"}
		default:
			r.found = true
			mark = store.ScrapeMark{ItemId: c.Id, Status: "ok", Source: "tmdb"}
			if r.nfo {
				mark.Source = "nfo+tmdb"
			}
		}
	} else {
		mark = store.ScrapeMark{ItemId: c.Id, Status: "ok", Source: "nfo"}
	}

	r.applyErr = s.db.ApplyMeta(ctx, patch, mark)
	if r.applyErr == nil && mark.Status == "ok" {
		s.writeNFO(c, patch)
		// NFO 完整时上面不会请求 TMDB，但分集（名称/简介/缩略图）仍然要以 TMDB 为准补齐。
		if c.Type == "Series" && !episodesDone && s.tmdb.Enabled() {
			s.fillSeriesEpisodes(ctx, c, patch, &r)
		}
	}
	return r
}

// RunItem 只刮削一个条目（Web「重新刮削」按钮使用）。
func (s *Scraper) RunItem(ctx context.Context, id string) (Result, error) {
	start := time.Now()
	res := Result{}
	defer func() { res.Elapsed = time.Since(start) }()

	c, err := s.db.ScrapeCandidateByID(ctx, id)
	if err != nil {
		return res, err
	}
	r := s.scrapeOne(ctx, *c)
	res.Processed = 1
	res.Images = r.images
	switch {
	case r.err != nil:
		res.Failed = 1
	case r.applyErr != nil:
		// 元数据没写进库（条目可能已被删除/ID 变更）：按失败上报，不能当成功。
		res.Failed = 1
	case r.nfo && !r.found:
		res.FromNFO = 1
	case r.found:
		res.FromTMDB = 1
	default:
		res.NotFound = 1
	}
	if r.err != nil {
		return res, r.err
	}
	return res, r.applyErr
}

// local 读 NFO 与本地图片，返回可直接落库的 patch。
func (s *Scraper) local(c store.ScrapeCandidate) (store.MetaPatch, store.ScrapeMark, bool) {
	p := store.MetaPatch{Id: c.Id}
	if s.opt.Root == "" || c.Path == "" {
		return p, store.ScrapeMark{}, false
	}
	dir := s.itemDir(c)
	if dir == "" {
		return p, store.ScrapeMark{}, false
	}
	stem := strings.TrimSuffix(filepath.Base(c.Path), filepath.Ext(c.Path))
	var names []string
	switch c.Type {
	case "Episode":
		names = []string{stem + ".nfo"}
	case "Series":
		names = []string{"tvshow.nfo"}
	default:
		names = []string{stem + ".nfo", "movie.nfo"}
	}

	var data *nfo.Data
	for _, n := range names {
		d, err := nfo.Load(filepath.Join(dir, n))
		if err == nil {
			data = d
			break
		}
		if !errors.Is(err, nfo.ErrNotFound) {
			// 损坏的 NFO 不阻塞：继续找下一个名字，最后仍走 TMDB。
			s.opt.Logger.Printf("scrape: nfo %s: %v", filepath.Join(dir, n), err)
		}
	}
	if data == nil {
		return p, store.ScrapeMark{}, false
	}
	applyNFO(&p, data, c)
	p.ImageRef = localImage(dir, "poster", "folder", "cover")
	p.BackdropRef = localImage(dir, "fanart", "backdrop", "background", "art")
	return p, store.ScrapeMark{ItemId: c.Id, Status: "ok", Source: "nfo"}, true
}

// keepNFOGaps 以 base（NFO）为骨架，只采纳来自 TMDB 的空缺字段：
// 标题/简介/年份/时长/评分以 NFO 为准，演职员与图片允许 TMDB 补。
func keepNFOGaps(base, tmdbPatch store.MetaPatch) store.MetaPatch {
	out := tmdbPatch
	if base.Name != "" {
		out.Name = base.Name
	}
	if base.Overview != "" {
		out.Overview = base.Overview
	}
	if base.ProductionYear > 0 {
		out.ProductionYear = base.ProductionYear
	}
	if base.RunTimeTicks > 0 {
		out.RunTimeTicks = base.RunTimeTicks
	}
	if base.CommunityRating > 0 {
		out.CommunityRating = base.CommunityRating
	}
	if base.PremiereDate != "" {
		out.PremiereDate = base.PremiereDate
	}
	if base.OriginalTitle != "" {
		out.OriginalTitle = base.OriginalTitle
	}
	if len(base.Genres) > 0 {
		out.Genres = base.Genres
	}
	if len(base.Tags) > 0 {
		out.Tags = base.Tags
	}
	if len(base.Studios) > 0 {
		out.Studios = base.Studios
	}
	if len(base.People) > 0 {
		out.People = base.People
	}
	if len(base.ProviderIds) > 0 {
		out.ProviderIds = base.ProviderIds
	}
	if len(base.Taglines) > 0 {
		out.Taglines = base.Taglines
	}
	if len(base.MediaStreams) > 0 {
		out.MediaStreams = base.MediaStreams
	}
	return out
}

// applyNFO 把 NFO 数据合并进 patch（只覆盖有值字段）。
func applyNFO(p *store.MetaPatch, d *nfo.Data, c store.ScrapeCandidate) {
	// 剧集：NFO 的 title 常是单集标题；Series 用 showtitle。
	if c.Type == "Episode" {
		if d.Title != "" {
			p.Name = d.Title
		}
		if d.ShowTitle != "" {
			p.SortName = fmt.Sprintf("%04d-%04d", d.Season, d.Episode)
		}
	} else if d.Title != "" {
		p.Name = d.Title
	}
	p.OriginalTitle = d.OriginalTitle
	if d.SortTitle != "" {
		p.ForcedSortName = d.SortTitle
	}
	p.Overview = d.Overview()
	p.PremiereDate = d.Premiered
	if d.Year > 0 {
		p.ProductionYear = d.Year
	}
	p.OfficialRating = d.MPAA
	p.CommunityRating = d.Rating
	p.CriticRating = d.CriticRating
	if d.RuntimeSeconds() > 0 {
		p.RunTimeTicks = int64(d.RuntimeSeconds()) * 10_000_000
	}
	if d.Genres != nil {
		p.Genres = d.Genres
	}
	if d.Tags != nil {
		p.Tags = d.Tags
	}
	if d.Studios != nil {
		for _, st := range d.Studios {
			p.Studios = append(p.Studios, model.NameId{Name: st, Id: nameID(st)})
		}
	}
	for _, name := range d.Directors {
		p.People = append(p.People, model.Person{Name: name, Id: nameID(name), Type: "Director"})
	}
	for _, name := range d.Writers {
		p.People = append(p.People, model.Person{Name: name, Id: nameID(name), Type: "Writer"})
	}
	for i, a := range d.Actors {
		p.People = append(p.People, model.Person{Name: a.Name, Id: nameID(a.Name), Role: a.Role, Type: "Actor"})
		if i > 60 {
			break
		}
	}
	if d.UniqueIDs != nil {
		ids := map[string]string{}
		for k, v := range d.UniqueIDs {
			ids[k] = v
		}
		p.ProviderIds = ids
	}
	if d.Tagline != "" {
		p.Taglines = []string{d.Tagline}
	}
	for _, st := range d.Streams {
		p.MediaStreams = append(p.MediaStreams, model.MediaStream{
			Type: st.Type, Codec: st.Codec, Width: st.Width, Height: st.Height,
			Channels: st.Channels, Language: st.Language, Title: st.Title,
			IsDefault: st.Default, IsForced: st.Forced,
		})
	}
}

// fillTMDB 请求 TMDB 并填充 patch；返回是否命中。
func (s *Scraper) fillTMDB(ctx context.Context, c store.ScrapeCandidate, p *store.MetaPatch, r *itemResult) (bool, error) {
	if !s.tmdb.Enabled() {
		return false, nil
	}
	switch c.Type {
	case "Movie":
		id := tmdbID(c.ProviderIds)
		if id == 0 {
			hit, err := s.search(ctx, "movie", c.Name, c.Year)
			if err != nil {
				return false, err
			}
			id = hit
		}
		if id == 0 {
			return false, nil
		}
		m, err := s.tmdb.MovieDetail(ctx, id)
		if errors.Is(err, tmdb.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		applyMovie(p, m)
		s.fetchImages(ctx, c, pickImage(m.Images.Posters, m.PosterPath), pickBackdrop(m.Images.Backdrops, m.BackdropPath), p, r)
		if p.LogoRef == "" {
			if ref, n := s.saveImage(ctx, c.Id, "logo", pickLogo(m.Images)); ref != "" {
				p.LogoRef = ref
				r.images += n
				if n > 0 && s.opt.Logger != nil {
					s.opt.Logger.Printf("  Logo：已写入 %s", filepath.Base(ref))
				}
			}
		}
		return true, nil

	case "Series":
		id := tmdbID(c.ProviderIds)
		if id == 0 {
			hit, err := s.search(ctx, "tv", c.Name, c.Year)
			if err != nil {
				return false, err
			}
			id = hit
		}
		if id == 0 {
			return false, nil
		}
		tv, err := s.tmdb.SeriesDetail(ctx, id)
		if errors.Is(err, tmdb.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		applySeries(p, tv)
		s.fetchImages(ctx, c, pickImage(tv.Images.Posters, tv.PosterPath), pickBackdrop(tv.Images.Backdrops, tv.BackdropPath), p, r)
		if p.LogoRef == "" {
			if ref, n := s.saveImage(ctx, c.Id, "logo", pickLogo(tv.Images)); ref != "" {
				p.LogoRef = ref
				r.images += n
				if n > 0 && s.opt.Logger != nil {
					s.opt.Logger.Printf("  Logo：已写入 %s", filepath.Base(ref))
				}
			}
		}
		s.saveSeasonPosters(ctx, c.Id, tv, r)
		// ★ 分集刮削：按 TMDB 的 Series → Season → Episode 层级补齐每集
		// 名称/简介/时长/缩略图。每次季详情请求覆盖整季，比逐集请求省 20~40 倍配额。
		s.fillEpisodes(ctx, c.Id, tv, r)
		return true, nil

	}
	return false, nil
}

// episodeSeriesID 找单集所属剧集的 TMDB id：父剧已刮削则直接用，否则按 series_name 搜索。
func (s *Scraper) episodeSeriesID(ctx context.Context, c store.ScrapeCandidate) (int64, error) {
	if v, _ := strconv.ParseInt(c.SeriesTmdb, 10, 64); v > 0 {
		return v, nil
	}
	name := c.SeriesName
	if name == "" {
		name = c.Name
	}
	return s.search(ctx, "tv", name, 0)
}

// search 返回最佳命中的 TMDB id；无够格命中返回 0。
func (s *Scraper) search(ctx context.Context, kind, title string, year int) (int64, error) {
	title = cleanTitle(title)
	if title == "" {
		return 0, nil
	}
	var results []tmdb.SearchResult
	var err error
	if kind == "movie" {
		results, err = s.tmdb.SearchMovie(ctx, title, year)
	} else {
		results, err = s.tmdb.SearchTV(ctx, title, year)
	}
	if err != nil {
		return 0, err
	}
	for _, r := range results {
		if !similar(title, r) {
			continue
		}
		return r.ID, nil
	}
	return 0, nil
}

// similar 判断搜索结果是否可信：标题归一化后相等或互为前缀，且不能是成人片。
// 过松的匹配（"沙丘" → "沙丘之子"）会把整部剧的元数据写错，比 notfound 更糟。
func similar(query string, r tmdb.SearchResult) bool {
	names := []string{r.Title, r.Name, r.OriginalTitle, r.OriginalName}
	q := normTitle(query)
	for _, n := range names {
		if normTitle(n) == q {
			return true
		}
	}
	return false
}

// fetchImages 下载海报/背景图到本地缓存并写 ref。
func (s *Scraper) fetchImages(ctx context.Context, c store.ScrapeCandidate, poster, backdrop string, p *store.MetaPatch, r *itemResult) {
	if s.opt.ImageDir == "" {
		return
	}
	if p.ImageRef == "" && poster != "" {
		if ref, n := s.saveImage(ctx, c.Id, "poster", poster); ref != "" {
			p.ImageRef = ref
			r.images += n
			if n > 0 && s.opt.Logger != nil {
				s.opt.Logger.Printf("  封面：已写入 %s", filepath.Base(ref))
			}
		}
	}
	if p.BackdropRef == "" && backdrop != "" {
		if ref, n := s.saveImage(ctx, c.Id, "backdrop", backdrop); ref != "" {
			p.BackdropRef = ref
			r.images += n
			if n > 0 && s.opt.Logger != nil {
				s.opt.Logger.Printf("  背景图：已写入 %s", filepath.Base(ref))
			}
		}
	}
}

// pickBackdrop 背景图按“分辨率优先”挑：面积最大者胜，同面积再看语言与评分。
// 详情页背景铺满屏幕，清晰度比文字语言重要，所以不复用 poster 的语言优先规则。
func pickBackdrop(list []tmdb.ImageItem, fallback string) string {
	best, bestScore := "", -1.0
	for _, it := range list {
		if it.FilePath == "" {
			continue
		}
		lang := 0.0
		switch strings.ToLower(strings.TrimSpace(it.Iso639)) {
		case "":
			lang = 3 // 无文字背景最适合做底图
		case "zh", "zh-cn", "zh-hans", "cn", "chs":
			lang = 2
		case "en":
			lang = 1
		}
		// 面积是第一权重（1e6 放大），语言/评分只做同分辨率排序。
		score := float64(it.Width)*float64(it.Height)*1e6 + lang*1e3 + it.Vote
		if score > bestScore {
			bestScore, best = score, it.FilePath
		}
	}
	if best == "" {
		return fallback
	}
	return best
}

// pickImage 在候选图片里挑一张：中文简体优先，其次无文字图，再次英文，
// 同语言内取分辨率最高（面积最大）的那张；没有候选时用 fallback（TMDB 主图）。
// TMDB 的图片语言用 ISO 639-1：中文为 zh（简体），偶尔有 zh-CN/cn。
func pickImage(list []tmdb.ImageItem, fallback string) string {
	best, bestScore := "", -1.0
	for _, it := range list {
		if it.FilePath == "" {
			continue
		}
		var pri float64
		switch strings.ToLower(strings.TrimSpace(it.Iso639)) {
		case "zh", "zh-cn", "zh-hans", "cn", "chs":
			pri = 4
		case "":
			pri = 3 // 无文字版本，兼容性最好
		case "en":
			pri = 2
		default:
			pri = 1
		}
		score := pri*1e15 + float64(it.Width)*float64(it.Height) + it.Vote
		if score > bestScore {
			bestScore, best = score, it.FilePath
		}
	}
	if best == "" {
		return fallback
	}
	return best
}

// pickLogo 选一张 Logo：优先中文，其次英文，再退任意。
func pickLogo(im tmdb.Images) string {
	var best string
	for _, l := range im.Logos {
		if l.FilePath == "" {
			continue
		}
		switch l.Iso639 {
		case "zh":
			return l.FilePath
		case "en":
			if best == "" {
				best = l.FilePath
			}
		default:
			if best == "" {
				best = l.FilePath
			}
		}
	}
	return best
}

// fillSeriesEpisodes 以剧集整体为入口补充分集数据：先确定剧集 TMDB id，
// 再按季拉取详情回填。即使剧集本身走了 NFO，也会执行这一步。
func (s *Scraper) fillSeriesEpisodes(ctx context.Context, c store.ScrapeCandidate, p store.MetaPatch, r *itemResult) {
	id := tmdbID(p.ProviderIds)
	if id == 0 {
		id = tmdbID(c.ProviderIds)
	}
	if id == 0 {
		hit, err := s.search(ctx, "tv", c.Name, c.Year)
		if err == nil {
			id = hit
		}
	}
	if id == 0 {
		return
	}
	tv, err := s.tmdb.SeriesDetail(ctx, id)
	if err != nil {
		if s.opt.Logger != nil {
			s.opt.Logger.Printf("分集补充跳过 %s：%v", c.Name, err)
		}
		return
	}
	s.fillEpisodes(ctx, c.Id, tv, r)
}

// fillEpisodes 用 TMDB 季详情批量回填本地单集（名称/简介/时长/缩略图）。
// 只处理本地已存在的集；按季请求，单季一次 API 调用。
func (s *Scraper) fillEpisodes(ctx context.Context, seriesItemID string, tv *tmdb.Series, r *itemResult) {
	seasons, err := s.db.SeasonNumbersOf(ctx, seriesItemID)
	if err != nil || len(seasons) == 0 {
		return
	}
	idx, err := s.db.EpisodeIndex(ctx, seriesItemID)
	if err != nil || len(idx) == 0 {
		return
	}
	for _, season := range seasons {
		if err := ctx.Err(); err != nil {
			return
		}
		sd, err := s.tmdb.SeasonDetail(ctx, tv.ID, season)
		if err != nil {
			s.opt.Logger.Printf("分集刮削跳过 S%02d：%v", season, err)
			continue
		}
		for _, e := range sd.Episodes {
			id, ok := idx[[2]int{e.SeasonNumber, e.EpisodeNumber}]
			if !ok {
				continue
			}
			patch := store.MetaPatch{Id: id}
			applyEpisode(&patch, &e)
			if e.StillPath != "" {
				patch.StillPath = e.StillPath
			}
			if err := s.db.ApplyMeta(ctx, patch, store.ScrapeMark{
				ItemId: id, Status: "ok", Source: "tmdb",
			}); err != nil {
				s.opt.Logger.Printf("分集写入失败 %s S%02dE%02d：%v", seriesItemID, e.SeasonNumber, e.EpisodeNumber, err)
				continue
			}
			// 分集也要实时落 NFO：用条目自身路径（与媒体文件同目录）。
			if epPath, ok := s.episodePath(ctx, id); ok {
				s.writeNFO(store.ScrapeCandidate{Id: id, Type: "Episode", Path: epPath, Name: patch.Name}, patch)
			}
		}
		// 简单限速：季详情按 TMDB 速率上限排队，避免触发 429。
		epRate := s.opt.RatePerSec
		if epRate <= 0 {
			epRate = 4
		}
		if epRate > 15 {
			epRate = 15
		}
		time.Sleep(time.Duration(float64(time.Second) / epRate))
	}
}

// saveSeasonPosters 把剧集下每季的海报写到对应 Season 条目。
func (s *Scraper) saveSeasonPosters(ctx context.Context, seriesItemID string, tv *tmdb.Series, r *itemResult) {
	if s.opt.ImageDir == "" || len(tv.Seasons) == 0 {
		return
	}
	updater, ok := s.db.(interface {
		UpdateSeasonPoster(context.Context, string, int, string) error
	})
	if !ok {
		return
	}
	for _, sn := range tv.Seasons {
		if sn.PosterPath == "" || sn.SeasonNumber < 0 {
			continue
		}
		key := fmt.Sprintf("%s_s%d", seriesItemID, sn.SeasonNumber)
		ref, n := s.saveImage(ctx, key, "poster", sn.PosterPath)
		if ref == "" {
			continue
		}
		r.images += n
		_ = updater.UpdateSeasonPoster(ctx, seriesItemID, sn.SeasonNumber, ref)
	}
}

func (s *Scraper) saveImage(ctx context.Context, id, kind, tmdbPath string) (string, int) {
	dir := filepath.Join(s.opt.ImageDir, s.opt.ServerID)
	cached := filepath.Join(dir, fmt.Sprintf("%s_%s.jpg", id, strings.ToLower(kind)))
	if fi, err := os.Stat(cached); err == nil && fi.Size() > 0 && !s.opt.ForceImages {
		return filepath.Join(s.opt.ServerID, filepath.Base(cached)), 0
	}
	body, err := s.tmdb.DownloadImage(ctx, tmdbPath, "original")
	if err != nil || len(body) == 0 {
		return "", 0
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0
	}
	name := id + "_" + kind + ".jpg"
	final := filepath.Join(dir, name)
	tmp := final + ".tmp"
	// 原子替换：先写临时文件再 rename，客户端不会读到空/半截图导致封面闪烁。
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", 0
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return "", 0
	}
	return path.Join(s.opt.ServerID, name), 1
}

// ---- 映射 ----

func applyMovie(p *store.MetaPatch, m *tmdb.Movie) {
	if m.Title != "" {
		p.Name = m.Title
	}
	p.OriginalTitle = m.OriginalTitle
	if m.Overview != "" {
		p.Overview = m.Overview
	}
	p.PremiereDate = m.ReleaseDate
	if y := yearOfDate(m.ReleaseDate); y > 0 {
		p.ProductionYear = y
	}
	if m.Runtime > 0 {
		p.RunTimeTicks = int64(m.Runtime) * 60 * 10_000_000
	}
	if m.VoteAverage > 0 {
		p.CommunityRating = round1(m.VoteAverage)
	}
	if m.Tagline != "" {
		p.Taglines = []string{m.Tagline}
	}
	if len(m.Genres) > 0 {
		p.Genres = genreNames(m.Genres)
	}
	if len(m.ProductionCompanies) > 0 {
		for _, co := range m.ProductionCompanies {
			p.Studios = append(p.Studios, model.NameId{Name: co.Name, Id: nameID(co.Name)})
		}
	}
	p.ProviderIds = map[string]string{"tmdb": strconv.FormatInt(m.ID, 10)}
	if m.IMDBID != "" {
		p.ProviderIds["imdb"] = m.IMDBID
	}
	p.People = creditsToPeople(m.Credits)

	for _, k := range m.Keywords.Keywords {
		if k.Name != "" {
			p.Tags = append(p.Tags, k.Name)
		}
	}
	if r := pickCertification(m.ReleaseDates); r != "" {
		p.OfficialRating = r
	}
	if m.BelongsToCollection != nil && m.BelongsToCollection.Name != "" {
		p.Tags = append(p.Tags, m.BelongsToCollection.Name)
	}
	var countries []string
	for _, c := range m.ProductionCountries {
		if c.Name != "" {
			countries = append(countries, c.Name)
		}
	}
	p.Tags = append(p.Tags, derivedTags(p.ProductionYear, countries)...)
}

// externalIDString 把 TMDB 的 external_ids 值（可能是数字或字符串）统一成字符串。
func externalIDString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(t)
	}
}

// pickCertification 从 release_dates 里挑一个分级：优先美国，其次中国。
func pickCertification(rd tmdb.ReleaseDates) string {
	for _, want := range []string{"CN", "HK", "TW", "US", "GB", "JP"} {
		for _, c := range rd.Results {
			if c.Iso3166 != want {
				continue
			}
			for _, d := range c.ReleaseDates {
				if d.Certification != "" {
					return d.Certification
				}
			}
		}
	}
	return ""
}

func applySeries(p *store.MetaPatch, tv *tmdb.Series) {
	if tv.Name != "" {
		p.Name = tv.Name
	}
	p.OriginalTitle = tv.OriginalName
	if tv.Overview != "" {
		p.Overview = tv.Overview
	}
	p.PremiereDate = tv.FirstAirDate
	if y := yearOfDate(tv.FirstAirDate); y > 0 {
		p.ProductionYear = y
	}
	if tv.VoteAverage > 0 {
		p.CommunityRating = round1(tv.VoteAverage)
	}
	if len(tv.Genres) > 0 {
		p.Genres = genreNames(tv.Genres)
	}
	for _, n := range tv.Networks {
		p.Studios = append(p.Studios, model.NameId{Name: n.Name, Id: nameID(n.Name)})
	}
	ids := map[string]string{"tmdb": strconv.FormatInt(tv.ID, 10)}
	if v := externalIDString(tv.ExternalIDs["imdb_id"]); v != "" {
		ids["imdb"] = v
	}
	if v := externalIDString(tv.ExternalIDs["tvdb_id"]); v != "" {
		ids["tvdb"] = v
	}
	p.ProviderIds = ids
	p.People = creditsToPeople(tv.Credits)

	for _, k := range tv.Keywords.Results {
		if k.Name != "" {
			p.Tags = append(p.Tags, k.Name)
		}
	}
	for _, want := range []string{"CN", "HK", "TW", "US", "GB", "JP"} {
		for _, c := range tv.ContentRatings.Results {
			if c.Iso3166 == want && c.Rating != "" {
				p.OfficialRating = c.Rating
				break
			}
		}
		if p.OfficialRating != "" {
			break
		}
	}
	var countries []string
	for _, c := range tv.ProductionCountries {
		if c.Name != "" {
			countries = append(countries, c.Name)
		}
	}
	p.Tags = append(p.Tags, derivedTags(p.ProductionYear, countries)...)
}

// derivedTags 生成可筛选的派生标签：年代（如 "2010年代"）与出品地区。
// 类型不放进 Tags（Genres 已经承载），避免客户端筛选时重复。
func derivedTags(year int, countries []string) []string {
	out := make([]string, 0, len(countries)+1)
	if year > 0 {
		out = append(out, fmt.Sprintf("%d年代", year/10*10))
	}
	seen := map[string]bool{}
	for _, c := range countries {
		if c == "" {
			continue
		}
		if zh, ok := countryZH[c]; ok {
			c = zh
		}
		if seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out
}

// countryZH 把 TMDB 的英文国名映射成中文，客户端筛选时更直观；未收录的保留原名。
var countryZH = map[string]string{
	"China":                    "中国",
	"Hong Kong":                "中国香港",
	"Taiwan":                   "中国台湾",
	"United States of America": "美国",
	"United Kingdom":           "英国",
	"Japan":                    "日本",
	"South Korea":              "韩国",
	"Korea":                    "韩国",
	"France":                   "法国",
	"Germany":                  "德国",
	"India":                    "印度",
	"Thailand":                 "泰国",
	"Canada":                   "加拿大",
	"Australia":                "澳大利亚",
	"Spain":                    "西班牙",
	"Italy":                    "意大利",
}

func applyEpisode(p *store.MetaPatch, e *tmdb.Episode) {
	if e.Name != "" {
		p.Name = e.Name
	}
	if e.Overview != "" {
		p.Overview = e.Overview
	}
	p.PremiereDate = e.AirDate
	if e.Runtime > 0 {
		p.RunTimeTicks = int64(e.Runtime) * 60 * 10_000_000
	}
	if e.VoteAverage > 0 {
		p.CommunityRating = round1(e.VoteAverage)
	}
	p.ProviderIds = map[string]string{"tmdb": strconv.FormatInt(e.ID, 10)}
	p.People = creditsToPeople(e.Credits)
}

func creditsToPeople(c tmdb.Credits) []model.Person {
	var out []model.Person
	seen := map[int64]bool{}
	for _, m := range c.Cast {
		if seen[m.ID] || len(out) > 40 {
			continue
		}
		seen[m.ID] = true
		out = append(out, model.Person{Name: m.Name, Id: nameID(m.Name), Role: m.Character, Type: "Actor"})
	}
	for _, m := range c.Crew {
		if len(out) > 60 {
			break
		}
		var t string
		switch m.Job {
		case "Director":
			t = "Director"
		case "Writer", "Screenplay", "Story":
			t = "Writer"
		case "Producer", "Executive Producer":
			t = "Producer"
		}
		if t == "" {
			continue
		}
		out = append(out, model.Person{Name: m.Name, Id: nameID(m.Name), Type: t})
	}
	return out
}

func genreNames(gs []tmdb.Genre) []string {
	out := make([]string, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.Name)
	}
	return out
}

// ---- 目录/图片工具 ----

// itemDir 返回条目所在目录（路径必须在 MediaRoot 内）。
func (s *Scraper) itemDir(c store.ScrapeCandidate) string {
	p := c.Path
	if !filepath.IsAbs(p) {
		p = filepath.Join(s.opt.Root, p)
	}
	p = filepath.Clean(p)
	root := filepath.Clean(s.opt.Root)
	rel, err := filepath.Rel(root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	fi, err := os.Stat(p)
	if err != nil {
		return ""
	}
	if fi.IsDir() {
		return p
	}
	return filepath.Dir(p)
}

// localImage 在目录里按候选名找图片，返回相对 MediaRoot 的路径（供图片端点用）。
func localImage(dir string, names ...string) string {
	for _, base := range names {
		for _, ext := range []string{".jpg", ".jpeg", ".png", ".webp"} {
			p := filepath.Join(dir, base+ext)
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
				return p
			}
			// Kodi 常见 -poster / -fanart 变体。
			p = filepath.Join(dir, base+"-"+ext[1:])
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
				return p
			}
		}
	}
	return ""
}

// tmdbID 从 provider ids 里取数字 id。
func tmdbID(ids map[string]string) int64 {
	for _, k := range []string{"tmdb", "Tmdb", "TMDB"} {
		if v, ok := ids[k]; ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

func nameID(name string) string {
	h := fnv.New64a()
	io.WriteString(h, strings.ToLower(strings.TrimSpace(name)))
	return fmt.Sprintf("%016x", h.Sum64())
}

func yearOfDate(d string) int {
	if len(d) < 4 {
		return 0
	}
	y, _ := strconv.Atoi(d[:4])
	return y
}

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

// cleanTitle 去掉常见的发布组后缀，只保留可搜索的部分。
func cleanTitle(s string) string {
	s = strings.TrimSpace(s)
	for _, sep := range []string{"【", "[", "（", "("} {
		if i := strings.Index(s, sep); i > 0 && strings.ContainsAny(s[i:], "0120") {
			s = strings.TrimSpace(s[:i])
			break
		}
	}
	return strings.TrimSpace(s)
}

// normTitle 归一化标题用于相似度判断：小写、去空格/标点/全角括号。
func normTitle(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 0x4e00 && r <= 0x9fff: // 中日韩统一表意文字
			b.WriteRune(r)
		case r >= 0x3040 && r <= 0x30ff: // 假名
			b.WriteRune(r)
		case r >= 0xac00 && r <= 0xd7af: // 谚文
			b.WriteRune(r)
		}
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// rl 把每秒速率换算成请求间隔。
func rl(perSec float64) time.Duration {
	if perSec <= 0 {
		perSec = 4
	}
	d := time.Duration(float64(time.Second) / perSec)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// NumWorkers 返回默认并发数（受 CPU 与速率共同约束）。
func NumWorkers() int {
	n := runtime.NumCPU() / 2
	if n < 1 {
		n = 1
	}
	if n > 4 {
		n = 4
	}
	return n
}

// isFatalTMDBError 判断是否属于「继续重试没有意义」的错误（密钥无效/被封）。
func isFatalTMDBError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "401") || strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "403") || strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "invalid api key")
}

// writeNFO 实时把元数据写成 Kodi 兼容的 .nfo（与媒体同目录）。
// 说明：Emby 的 NFO 写入是“扫描/刷新时批量落盘”，这里在刮削成功后立即写，避免下次扫描才见。
func (s *Scraper) writeNFO(c store.ScrapeCandidate, p store.MetaPatch) {
	if !s.opt.UseNFO || c.Path == "" || (p.Name == "" && p.Overview == "") {
		return
	}
	var target string
	switch c.Type {
	case "Movie", "Episode":
		ext := filepath.Ext(c.Path)
		if ext == "" { // 已经是目录（少见）：写到目录内
			target = filepath.Join(c.Path, "movie.nfo")
		} else {
			target = strings.TrimSuffix(c.Path, ext) + ".nfo"
		}
	case "Series":
		target = filepath.Join(c.Path, "tvshow.nfo")
	default:
		return
	}
	if target == "" {
		return
	}
	root := strings.TrimRight(s.opt.Root, "/")
	if root != "" && !strings.HasPrefix(target, root+"/") {
		return // 只写媒体根内的文件
	}

	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n")
	switch c.Type {
	case "Episode":
		b.WriteString("<episodedetails>\n")
	case "Series":
		b.WriteString("<tvshow>\n")
	default:
		b.WriteString("<movie>\n")
	}
	esc := func(v string) string { return xmlEscape(v) }
	b.WriteString("  <title>" + esc(p.Name) + "</title>\n")
	if p.OriginalTitle != "" {
		b.WriteString("  <originaltitle>" + esc(p.OriginalTitle) + "</originaltitle>\n")
	}
	if p.Overview != "" {
		b.WriteString("  <plot>" + esc(p.Overview) + "</plot>\n")
	}
	if p.ProductionYear > 0 {
		b.WriteString(fmt.Sprintf("  <year>%d</year>\n", p.ProductionYear))
	}
	if p.PremiereDate != "" {
		b.WriteString("  <premiered>" + esc(p.PremiereDate) + "</premiered>\n")
	}
	if p.CommunityRating > 0 {
		b.WriteString(fmt.Sprintf("  <rating>%.1f</rating>\n", p.CommunityRating))
	}
	if p.RunTimeTicks > 0 {
		b.WriteString(fmt.Sprintf("  <runtime>%d</runtime>\n", p.RunTimeTicks/600_000_000))
	}
	for _, g := range p.Genres {
		b.WriteString("  <genre>" + esc(g) + "</genre>\n")
	}
	if id := p.ProviderIds["tmdb"]; id != "" {
		b.WriteString("  <uniqueid type=\"tmdb\" default=\"true\">" + esc(id) + "</uniqueid>\n")
	}
	for _, a := range p.People {
		if a.Type != "Actor" {
			continue
		}
		b.WriteString("  <actor><name>" + esc(a.Name) + "</name><role>" + esc(a.Role) + "</role></actor>\n")
	}
	if c.Type == "Series" {
		b.WriteString("</tvshow>\n")
	} else if c.Type == "Episode" {
		b.WriteString("</episodedetails>\n")
	} else {
		b.WriteString("</movie>\n")
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return
	}
	if err := os.WriteFile(target, []byte(b.String()), 0o644); err != nil {
		if s.opt.Logger != nil {
			s.opt.Logger.Printf("NFO 写入失败 %s：%v", target, err)
		}
		return
	}
	if s.opt.Logger != nil {
		s.opt.Logger.Printf("  NFO：已写入 %s", filepath.Base(target))
	}
}

// xmlEscape 转义 XML 文本节点。
func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")
	return r.Replace(s)
}

// episodePath 取单集的本地 .strm 路径（用于写 NFO）。
func (s *Scraper) episodePath(ctx context.Context, id string) (string, bool) {
	p := s.db.ItemPath(ctx, id)
	return p, p != ""
}
