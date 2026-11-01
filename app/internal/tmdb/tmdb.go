// Package tmdb 是 TMDB v3 API 的最小客户端。
//
// 设计要点：
//   - 每个响应都以 JSON 落盘缓存（<cacheDir>/<kind>/<id>-<lang>.json），失败也负缓存；
//     默认正缓存 30 天、负缓存 7 天，过期后自动重取。重新刮削用 WithForceRefresh
//     绕过缓存，保证强制刷新确实会打 TMDB。
//   - 429 / 5xx 走指数退避重试；Retry-After 优先。其它 4xx 直接返回错误，不重试。
//   - WithRate 在 Client 内做全局限速；API 详情、季集和图片下载共用同一个令牌桶，
//     避免单条内并发请求绕过按条目限速。
//   - 图片只在需要时下载（DownloadImage），并同样缓存到磁盘。
package tmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ErrNotFound 表示 TMDB 明确回答 404。
var ErrNotFound = errors.New("tmdb: not found")

// Client 是 TMDB 客户端。零值不可用，必须经 New 构造。
type Client struct {
	apiKey      string
	baseURL     string // 形如 https://api.themoviedb.org/3（测试可替换）
	imageURL    string // 形如 https://image.tmdb.org/t/p
	lang        string // zh-CN
	fallback    string // en-US
	cacheDir    string
	http        *http.Client
	maxRetry    int
	cacheTTL    time.Duration
	negativeTTL time.Duration
	imageTTL    time.Duration
	force       bool
	forceImages bool
	limiter     *time.Ticker
}

// Options 控制客户端行为。
type Options struct {
	APIKey       string
	BaseURL      string // 可空
	ImageBaseURL string // 可空
	Language     string // 默认 zh-CN
	FallbackLang string // 默认 en-US
	CacheDir     string // 空表示不缓存
	Proxy        string // 形如 http://host:port；空则用环境变量代理
	NoProxy      string // 逗号分隔的直连主机/后缀
	Timeout      time.Duration
	MaxRetry     int
	// 缓存有效期（从文件修改时间算）；<=0 表示不读取旧缓存。
	CacheTTL         time.Duration
	NegativeCacheTTL time.Duration
	ImageCacheTTL    time.Duration
}

// New 构造客户端。
func New(o Options) *Client {
	if o.BaseURL == "" {
		o.BaseURL = "https://api.themoviedb.org/3"
	}
	if o.ImageBaseURL == "" {
		o.ImageBaseURL = "https://image.tmdb.org/t/p"
	}
	if o.Language == "" {
		o.Language = "zh-CN"
	}
	if o.FallbackLang == "" {
		o.FallbackLang = "en-US"
	}
	if o.Timeout <= 0 {
		o.Timeout = 15 * time.Second
	}
	if o.MaxRetry <= 0 {
		o.MaxRetry = 3
	}
	if o.CacheTTL == 0 {
		o.CacheTTL = 30 * 24 * time.Hour
	}
	if o.NegativeCacheTTL == 0 {
		o.NegativeCacheTTL = 7 * 24 * time.Hour
	}
	if o.ImageCacheTTL == 0 {
		o.ImageCacheTTL = 30 * 24 * time.Hour
	}
	return &Client{
		apiKey:   o.APIKey,
		baseURL:  strings.TrimRight(o.BaseURL, "/"),
		imageURL: strings.TrimRight(o.ImageBaseURL, "/"),
		lang:     o.Language,
		fallback: o.FallbackLang,
		cacheDir: o.CacheDir,
		// 走环境变量代理：NAS 在墙内，容器只有经代理才能访问 TMDB。
		http: &http.Client{
			Timeout:   o.Timeout,
			Transport: &http.Transport{Proxy: proxyFunc(o.Proxy, o.NoProxy)},
		},
		maxRetry:    o.MaxRetry,
		cacheTTL:    o.CacheTTL,
		negativeTTL: o.NegativeCacheTTL,
		imageTTL:    o.ImageCacheTTL,
	}
}

// proxyFunc 返回带可选代理和 NO_PROXY 规则的 Proxy 函数。
// 不配置代理时保持 Go 默认的 HTTP_PROXY/HTTPS_PROXY 行为。
func proxyFunc(proxy, noProxy string) func(*http.Request) (*url.URL, error) {
	base := http.ProxyFromEnvironment
	if strings.TrimSpace(proxy) != "" {
		if u, err := url.Parse(strings.TrimSpace(proxy)); err == nil && u.Scheme != "" && u.Host != "" {
			base = http.ProxyURL(u)
		}
	}
	rules := make([]string, 0)
	for _, r := range strings.Split(noProxy, ",") {
		if r = strings.TrimSpace(strings.ToLower(r)); r != "" {
			rules = append(rules, r)
		}
	}
	if len(rules) == 0 {
		return base
	}
	return func(req *http.Request) (*url.URL, error) {
		host := strings.ToLower(req.URL.Hostname())
		for _, r := range rules {
			if r == "*" || host == r || strings.HasSuffix(host, "."+r) {
				return nil, nil
			}
		}
		return base(req)
	}
}

// WithForceRefresh 返回一个忽略 API/图片磁盘缓存的副本（用于“重新刮削/全量”）。
func (c *Client) WithForceRefresh() *Client {
	if c == nil {
		return nil
	}
	cp := *c
	cp.force = true
	return &cp
}

// WithForceImages 只忽略图片缓存，保留 API 元数据缓存（补封面时不重复拉详情）。
func (c *Client) WithForceImages() *Client {
	if c == nil {
		return nil
	}
	cp := *c
	cp.forceImages = true
	return &cp
}

// WithRate 返回一个带全局限速的副本（按秒请求数）。<=0 表示不限速。
func (c *Client) WithRate(perSecond float64) *Client {
	if c == nil || perSecond <= 0 {
		return c
	}
	cp := *c
	interval := time.Duration(float64(time.Second) / perSecond)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	cp.limiter = time.NewTicker(interval)
	return &cp
}

// wait 在每次真实网络请求前取一个限速令牌。
func (c *Client) wait(ctx context.Context) error {
	if c == nil || c.limiter == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.limiter.C:
		return nil
	}
}

// cacheFresh 判断缓存文件是否在 TTL 内；force 时永远不新鲜。
func (c *Client) cacheFresh(path string, ttl time.Duration) bool {
	if c == nil || c.force || ttl <= 0 {
		return false
	}
	fi, err := os.Stat(path)
	return err == nil && time.Since(fi.ModTime()) <= ttl
}

// imageCacheFresh 与 cacheFresh 类似，但补封面只需要跳过图片缓存。
func (c *Client) imageCacheFresh(path string, ttl time.Duration) bool {
	if c == nil || c.force || c.forceImages || ttl <= 0 {
		return false
	}
	fi, err := os.Stat(path)
	return err == nil && time.Since(fi.ModTime()) <= ttl
}

// Enabled 表示配置了可用凭据。
func (c *Client) Enabled() bool { return c != nil && c.apiKey != "" }

// ---- 数据模型 ----

// Movie 是 /movie/{id} 的子集。
type keywordItem struct {
	Name string `json:"name"`
}

type movieKeywords struct {
	Keywords []keywordItem `json:"keywords"`
}

type tvKeywords struct {
	Results []keywordItem `json:"results"`
}

type ratingItem struct {
	Iso3166 string `json:"iso_3166_1"`
	Rating  string `json:"rating"`
}

type ContentRatings struct {
	Results []ratingItem `json:"content_ratings"`
}

type ReleaseDates struct {
	Results []struct {
		Iso3166      string `json:"iso_3166_1"`
		ReleaseDates []struct {
			Certification string `json:"certification"`
		} `json:"release_dates"`
	} `json:"results"`
}

type ImageItem struct {
	FilePath string  `json:"file_path"`
	Iso639   string  `json:"iso_639_1"`
	Width    int     `json:"width"`
	Height   int     `json:"height"`
	Vote     float64 `json:"vote_average"`
}

type Images struct {
	Logos     []ImageItem `json:"logos"`
	Posters   []ImageItem `json:"posters"`
	Backdrops []ImageItem `json:"backdrops"`
}

type Country struct {
	Iso3166 string `json:"iso_3166_1"`
	Name    string `json:"name"`
}

type Collection struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Movie struct {
	ID                  int64         `json:"id"`
	Title               string        `json:"title"`
	OriginalTitle       string        `json:"original_title"`
	Overview            string        `json:"overview"`
	Tagline             string        `json:"tagline"`
	ReleaseDate         string        `json:"release_date"`
	Runtime             int           `json:"runtime"`
	VoteAverage         float64       `json:"vote_average"`
	VoteCount           int           `json:"vote_count"`
	Popularity          float64       `json:"popularity"`
	PosterPath          string        `json:"poster_path"`
	BackdropPath        string        `json:"backdrop_path"`
	Adult               bool          `json:"adult"`
	IMDBID              string        `json:"imdb_id"`
	Genres              []Genre       `json:"genres"`
	ProductionCompanies []Company     `json:"production_companies"`
	ProductionCountries []Country     `json:"production_countries"`
	Images              Images        `json:"images"`
	Credits             Credits       `json:"credits"`
	Keywords            movieKeywords `json:"keywords"`
	ReleaseDates        ReleaseDates  `json:"release_dates"`
	BelongsToCollection *Collection   `json:"belongs_to_collection"`
	Translations        struct {
		Translations []Translation `json:"translations"`
	} `json:"translations"`
}

// Series 是 /tv/{id} 的子集。
type Series struct {
	ID                  int64          `json:"id"`
	Name                string         `json:"name"`
	OriginalName        string         `json:"original_name"`
	Overview            string         `json:"overview"`
	Tagline             string         `json:"tagline"`
	FirstAirDate        string         `json:"first_air_date"`
	LastAirDate         string         `json:"last_air_date"`
	VoteAverage         float64        `json:"vote_average"`
	VoteCount           int            `json:"vote_count"`
	Popularity          float64        `json:"popularity"`
	PosterPath          string         `json:"poster_path"`
	BackdropPath        string         `json:"backdrop_path"`
	Adult               bool           `json:"adult"`
	NumberOfSeasons     int            `json:"number_of_seasons"`
	NumberOfEpisodes    int            `json:"number_of_episodes"`
	Genres              []Genre        `json:"genres"`
	Networks            []Company      `json:"networks"`
	ProductionCountries []Country      `json:"production_countries"`
	Images              Images         `json:"images"`
	OriginCountry       []string       `json:"origin_country"`
	ExternalIDs         map[string]any `json:"external_ids"`
	Seasons             []Season       `json:"seasons"`
	Credits             Credits        `json:"credits"`
	Keywords            tvKeywords     `json:"keywords"`
	ContentRatings      ContentRatings `json:"content_ratings"`
}

// Season 是剧集里的季摘要。
type Season struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	SeasonNumber int    `json:"season_number"`
	EpisodeCount int    `json:"episode_count"`
	AirDate      string `json:"air_date"`
	PosterPath   string `json:"poster_path"`
}

// SeasonDetail 是 /tv/{id}/season/{n}。
type SeasonDetail struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Overview     string    `json:"overview"`
	SeasonNumber int       `json:"season_number"`
	AirDate      string    `json:"air_date"`
	PosterPath   string    `json:"poster_path"`
	Episodes     []Episode `json:"episodes"`
	Credits      Credits   `json:"credits"`
}

// Episode 是 /tv/{id}/season/{n}/episode/{m} 或季度详情的子项。
type Episode struct {
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	Overview      string  `json:"overview"`
	EpisodeNumber int     `json:"episode_number"`
	SeasonNumber  int     `json:"season_number"`
	AirDate       string  `json:"air_date"`
	Runtime       int     `json:"runtime"`
	VoteAverage   float64 `json:"vote_average"`
	VoteCount     int     `json:"vote_count"`
	StillPath     string  `json:"still_path"`
	Credits       Credits `json:"credits"`
}

// Genre 是类型。
type Genre struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Company 是公司/电视网。
type Company struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	LogoPath string `json:"logo_path"`
}

// Credits 是演员/剧组。
type Credits struct {
	Cast []CastMember `json:"cast"`
	Crew []CrewMember `json:"crew"`
}

// CastMember 是一名演员。
type CastMember struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Character   string `json:"character"`
	Order       int    `json:"order"`
	ProfilePath string `json:"profile_path"`
}

// CrewMember 是一名剧组成员。
type CrewMember struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Job         string `json:"job"`
	Department  string `json:"department"`
	ProfilePath string `json:"profile_path"`
}

// Translation 是一条翻译摘要（只有语言代码与标题）。
type Translation struct {
	ISO6391     string `json:"iso_639_1"`
	ISO31661    string `json:"iso_3166_1"`
	Name        string `json:"name"`
	EnglishName string `json:"english_name"`
	Data        struct {
		Title    string `json:"title"`
		Name     string `json:"name"`
		Overview string `json:"overview"`
		Tagline  string `json:"tagline"`
	} `json:"data"`
}

// SearchResult 是 /search/{movie,tv} 的一条命中。
type SearchResult struct {
	ID            int64   `json:"id"`
	Title         string  `json:"title"`
	Name          string  `json:"name"`
	OriginalTitle string  `json:"original_title"`
	OriginalName  string  `json:"original_name"`
	ReleaseDate   string  `json:"release_date"`
	FirstAirDate  string  `json:"first_air_date"`
	Popularity    float64 `json:"popularity"`
	VoteCount     int     `json:"vote_count"`
	VoteAverage   float64 `json:"vote_average"`
	PosterPath    string  `json:"poster_path"`
}

// ---- 请求 ----

// MovieDetail 取电影详情（含演职员、翻译）。
func (c *Client) MovieDetail(ctx context.Context, id int64) (*Movie, error) {
	var m Movie
	if err := c.get(ctx, "movie", strconv.FormatInt(id, 10), c.lang,
		url.Values{"append_to_response": {"credits,translations,keywords,release_dates,images"}}, &m); err != nil {
		return nil, err
	}
	if m.Overview == "" || m.Title == "" {
		// 中文缺失时回退英文；同一 ID 不同语言各自缓存。
		// 回退失败（404/超时）不能把已有中文详情判成失败：TMDB 上很多条目
		// 只有一种语言的词条。只在“主语言什么都没拿到”时才向上报错。
		var fb Movie
		err := c.get(ctx, "movie", strconv.FormatInt(id, 10), c.fallback,
			url.Values{"append_to_response": {"credits,translations"}}, &fb)
		switch {
		case err == nil:
			mergeMovie(&m, &fb)
		case m.Title == "" && m.Overview == "":
			return nil, err
		}
	}
	return &m, nil
}

// SeriesDetail 取剧集详情。
func (c *Client) SeriesDetail(ctx context.Context, id int64) (*Series, error) {
	var s Series
	if err := c.get(ctx, "tv", strconv.FormatInt(id, 10), c.lang,
		url.Values{"append_to_response": {"credits,external_ids,keywords,content_ratings,images"}}, &s); err != nil {
		return nil, err
	}
	if s.Overview == "" || s.Name == "" {
		var fb Series
		if err := c.get(ctx, "tv", strconv.FormatInt(id, 10), c.fallback,
			url.Values{"append_to_response": {"credits,external_ids"}}, &fb); err == nil {
			mergeSeries(&s, &fb)
		}
	}
	return &s, nil
}

// SeasonDetail 取单季（含所有剧集）。
func (c *Client) SeasonDetail(ctx context.Context, seriesID int64, season int) (*SeasonDetail, error) {
	var sd SeasonDetail
	p := fmt.Sprintf("%d/season/%d", seriesID, season)
	if err := c.get(ctx, "tv", p, c.lang, url.Values{"append_to_response": {"credits"}}, &sd); err != nil {
		return nil, err
	}
	return &sd, nil
}

// EpisodeDetail 取单集（含演职员）。
func (c *Client) EpisodeDetail(ctx context.Context, seriesID int64, season, episode int) (*Episode, error) {
	var e Episode
	p := fmt.Sprintf("%d/season/%d/episode/%d", seriesID, season, episode)
	if err := c.get(ctx, "tv", p, c.lang, url.Values{"append_to_response": {"credits"}}, &e); err != nil {
		return nil, err
	}
	if e.Overview == "" {
		var fb Episode
		if err := c.get(ctx, "tv", p, c.fallback, url.Values{"append_to_response": {"credits"}}, &fb); err == nil && fb.Overview != "" {
			e.Overview = fb.Overview
		}
	}
	return &e, nil
}

// SearchMovie 按标题 + 年份搜索电影，返回按（年份吻合、热度）排序的候选。
func (c *Client) SearchMovie(ctx context.Context, title string, year int) ([]SearchResult, error) {
	return c.search(ctx, "movie", title, year)
}

// SearchTV 按标题 + 首播年份搜索剧集。
func (c *Client) SearchTV(ctx context.Context, title string, year int) ([]SearchResult, error) {
	return c.search(ctx, "tv", title, year)
}

func (c *Client) search(ctx context.Context, kind, title string, year int) ([]SearchResult, error) {
	q := url.Values{"query": {title}}
	if year > 0 {
		if kind == "movie" {
			q.Set("year", strconv.Itoa(year))
		} else {
			q.Set("first_air_date_year", strconv.Itoa(year))
		}
	}
	var out struct {
		Results []SearchResult `json:"results"`
	}
	key := "search:" + kind + ":" + strings.ToLower(title) + ":" + strconv.Itoa(year)
	if err := c.get(ctx, "search", kind, c.lang, q, &out, key); err != nil {
		return nil, err
	}
	results := out.Results
	// 年份精准命中优先，再按热度；避免 "沙丘 (1984)" 匹配到 2021 版。
	if year > 0 {
		var exact, other []SearchResult
		for _, r := range results {
			y := yearOf(r.ReleaseDate)
			if y == 0 {
				y = yearOf(r.FirstAirDate)
			}
			if y == year {
				exact = append(exact, r)
			} else {
				other = append(other, r)
			}
		}
		results = append(exact, other...)
	}
	return results, nil
}

// ImageURL 返回图片 URL；size 形如 "w500" / "original"。
func (c *Client) ImageURL(path, size string) string {
	if path == "" {
		return ""
	}
	if size == "" {
		size = "original"
	}
	return c.imageURL + "/" + size + "/" + strings.TrimPrefix(path, "/")
}

// DownloadImage 下载图片（带磁盘缓存），返回本地字节。
// 失败返回错误，调用方决定是否降级到占位图。
func (c *Client) DownloadImage(ctx context.Context, path, size string) ([]byte, error) {
	if path == "" {
		return nil, ErrNotFound
	}
	if size == "" {
		size = "original"
	}
	cacheFile := filepath.Join(c.cacheDir, "img", sanitize(size+"_"+strings.TrimPrefix(path, "/")))
	if c.cacheDir != "" && c.imageCacheFresh(cacheFile, c.imageTTL) {
		if b, err := os.ReadFile(cacheFile); err == nil {
			return b, nil
		}
	}
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	u := c.ImageURL(path, size)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tmdb: image %s: %s", u, res.Status)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if c.cacheDir != "" {
		_ = os.MkdirAll(filepath.Dir(cacheFile), 0o755)
		_ = os.WriteFile(cacheFile, body, 0o644)
	}
	return body, nil
}

// get 发请求并解码到 out。缓存键：kind/tail-land.json（search 另带 key）。
func (c *Client) get(ctx context.Context, kind, tail, lang string, q url.Values, out any, extraKey ...string) error {
	if !c.Enabled() {
		return errors.New("tmdb: no api key configured")
	}
	if q == nil {
		q = url.Values{}
	}
	q.Set("language", lang)
	u := c.baseURL + "/" + kind + "/" + strings.TrimPrefix(tail, "/") + "?" + q.Encode()

	var cacheFile string
	if c.cacheDir != "" {
		name := kind + "_" + tail + "_" + lang
		if len(extraKey) > 0 {
			// 搜索缓存键不能用 sanitize 后的标题：同长度中文标题会碰撞。
			// 用 FNV 哈希保留区分度，避免 A 的搜索结果被 B 复用。
			h := fnv.New32a()
			_, _ = h.Write([]byte(extraKey[0]))
			name = kind + "_search_" + strconv.FormatUint(uint64(h.Sum32()), 16)
		} else {
			// 把查询参数也纳入键：append_to_response 变化时必须重新取，否则会命中旧缓存。
			h := fnv.New32a()
			_, _ = h.Write([]byte(q.Encode()))
			name += "_" + strconv.FormatUint(uint64(h.Sum32()), 16)
		}
		name = strings.ReplaceAll(name, "/", "_") + ".json"
		cacheFile = filepath.Join(c.cacheDir, "api", sanitize(name))
		if b, err := os.ReadFile(cacheFile); err == nil {
			ttl := c.cacheTTL
			if string(b) == "null" {
				ttl = c.negativeTTL
			}
			if c.cacheFresh(cacheFile, ttl) {
				// 负缓存写成 null：必须还原成 ErrNotFound，否则坏 ID 会被当成空对象。
				if string(b) == "null" {
					return ErrNotFound
				}
				return json.Unmarshal(b, out)
			}
		}
	}

	body, err := c.doWithRetry(ctx, u)
	if err != nil {
		if errors.Is(err, ErrNotFound) && cacheFile != "" {
			// 404 负缓存成 null：断点续跑时不再对同一坏 ID 发请求。
			_ = os.MkdirAll(filepath.Dir(cacheFile), 0o755)
			_ = os.WriteFile(cacheFile, []byte("null"), 0o644)
		}
		return err
	}
	if string(body) == "null" {
		return ErrNotFound
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("tmdb: decode %s: %w", u, err)
	}
	if cacheFile != "" {
		_ = os.MkdirAll(filepath.Dir(cacheFile), 0o755)
		_ = os.WriteFile(cacheFile, body, 0o644)
	}
	return nil
}

func (c *Client) doWithRetry(ctx context.Context, u string) ([]byte, error) {
	var lastErr error
	backoff := 400 * time.Millisecond
	for attempt := range c.maxRetry {
		if err := c.wait(ctx); err != nil {
			return nil, err
		}
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		// v3 参数鉴权：api_key 放 query；不使用 Bearer，避免与 v4 token 混淆。
		q := req.URL.Query()
		q.Set("api_key", c.apiKey)
		req.URL.RawQuery = q.Encode()

		res, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, rerr := io.ReadAll(io.LimitReader(res.Body, 8<<20))
		res.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		switch {
		case res.StatusCode == http.StatusOK:
			return body, nil
		case res.StatusCode == http.StatusNotFound:
			return nil, ErrNotFound
		case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
			lastErr = fmt.Errorf("tmdb: %s: %s", u, res.Status)
			// 有 Retry-After 就用它；否则保持当前退避（下一轮再翻倍）。
			// 不能把它当“本轮的退避”再乘 2，否则 Retry-After 形同虚设。
			if ra := res.Header.Get("Retry-After"); ra != "" {
				// 0 也接受：TMDB 用它表示立即重试。
				if s, err := strconv.Atoi(ra); err == nil && s >= 0 && s < 60 {
					backoff = time.Duration(s) * time.Second
					continue
				}
			}
			backoff *= 2
			continue
		default:
			// 401/403 等：密钥问题，重试无意义。
			return nil, fmt.Errorf("tmdb: %s: %s", u, res.Status)
		}
	}
	return nil, lastErr
}

// ---- 合并 ----

func mergeMovie(dst, fb *Movie) {
	if dst.Title == "" {
		dst.Title = fb.Title
	}
	if dst.Overview == "" {
		dst.Overview = fb.Overview
	}
	if dst.Tagline == "" {
		dst.Tagline = fb.Tagline
	}
	if dst.OriginalTitle == "" {
		dst.OriginalTitle = fb.OriginalTitle
	}
	if dst.PosterPath == "" {
		dst.PosterPath = fb.PosterPath
	}
	if dst.BackdropPath == "" {
		dst.BackdropPath = fb.BackdropPath
	}
	if len(dst.Credits.Cast) == 0 {
		dst.Credits = fb.Credits
	}
	if len(dst.Genres) == 0 {
		dst.Genres = fb.Genres
	}
}

func mergeSeries(dst, fb *Series) {
	if dst.Name == "" {
		dst.Name = fb.Name
	}
	if dst.Overview == "" {
		dst.Overview = fb.Overview
	}
	if dst.Tagline == "" {
		dst.Tagline = fb.Tagline
	}
	if dst.OriginalName == "" {
		dst.OriginalName = fb.OriginalName
	}
	if dst.PosterPath == "" {
		dst.PosterPath = fb.PosterPath
	}
	if dst.BackdropPath == "" {
		dst.BackdropPath = fb.BackdropPath
	}
	if len(dst.Credits.Cast) == 0 {
		dst.Credits = fb.Credits
	}
	if len(dst.Genres) == 0 {
		dst.Genres = fb.Genres
	}
	if len(dst.Seasons) == 0 {
		dst.Seasons = fb.Seasons
	}
}

func yearOf(date string) int {
	if len(date) < 4 {
		return 0
	}
	y, _ := strconv.Atoi(date[:4])
	return y
}

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
