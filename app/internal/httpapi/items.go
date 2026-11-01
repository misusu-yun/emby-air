package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"emby-air/internal/model"
	"emby-air/internal/store"
)

// getCI 是大小写不敏感的 query 读取：客户端会传 parentId / ParentId / PARENTID。
// 找不到时返回空串。这是所有查询参数解析的唯一入口。
func getCI(r *http.Request, keys ...string) string {
	q := r.URL.Query()
	for _, k := range keys {
		if v, ok := q[k]; ok && len(v) > 0 {
			return v[0]
		}
	}
	lower := make([]string, 0, len(keys))
	for _, k := range keys {
		lower = append(lower, strings.ToLower(k))
	}
	for name, vals := range q {
		if len(vals) == 0 {
			continue
		}
		ln := strings.ToLower(name)
		for _, want := range lower {
			if ln == want {
				return vals[0]
			}
		}
	}
	return ""
}

// splitCSV 把 "A,B, C" 拆成去空白的非空元素。
func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// atoiDefault 解析整数，失败返回 def。
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return n
}

// boolValue 解析 true/false，缺失或非法返回 false。
func boolValue(s string) bool { return parseBoolPtr(s) != nil && *parseBoolPtr(s) }

// parseBoolPtr 解析 "true"/"false"，缺失返回 nil。
func parseBoolPtr(s string) *bool {
	if s == "" {
		return nil
	}
	b := strings.EqualFold(strings.TrimSpace(s), "true") || s == "1"
	return &b
}

// parseSortBy 展开 "SortName,DateCreated"，去掉 "-" 方向前缀
// （方向由 ItemsQuery.SortOrder 单独承载）。
func parseSortBy(sortBy string) []string {
	fields := splitCSV(sortBy)
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, strings.TrimPrefix(f, "-"))
	}
	return out
}

// parseYears 解析 "2020,2021,2019-2021" 这类年份列表。
func parseYears(s string) []int {
	var out []int
	for _, part := range splitCSV(s) {
		if i := strings.IndexByte(part, '-'); i > 0 {
			lo := atoiDefault(part[:i], 0)
			hi := atoiDefault(part[i+1:], 0)
			if lo > 0 && hi >= lo && hi-lo <= 200 {
				for y := lo; y <= hi; y++ {
					out = append(out, y)
				}
				continue
			}
		}
		if y := atoiDefault(part, 0); y > 0 {
			out = append(out, y)
		}
	}
	return out
}

// itemsQueryFrom 把请求 query 映射为 model.ItemsQuery。
// userId 为空时取当前用户。
func itemsQueryFrom(r *http.Request, userId string) model.ItemsQuery {
	if userId == "" {
		if u := userFrom(r); u != nil {
			userId = u.Id
		}
	}
	q := model.ItemsQuery{
		UserId:           userId,
		ParentId:         getCI(r, "parentId", "id", "topParentId"),
		Types:            splitCSV(getCI(r, "includeItemTypes", "includeItemType", "types")),
		ExcludeTypes:     splitCSV(getCI(r, "excludeItemTypes", "excludeItemType")),
		Recursive:        boolValue(getCI(r, "recursive")),
		SortBy:           parseSortBy(getCI(r, "sortBy")),
		SortOrder:        normalizeSortOrder(getCI(r, "sortOrder")),
		StartIndex:       atoiDefault(getCI(r, "startIndex"), 0),
		Limit:            atoiDefault(getCI(r, "limit"), 0),
		SearchTerm:       getCI(r, "searchTerm", "searchterm"),
		Filters:          splitCSV(getCI(r, "filters")),
		Genres:           splitCSV(getCI(r, "genres", "genreIds")),
		Years:            parseYears(getCI(r, "years", "year")),
		IsPlayed:         parseBoolPtr(getCI(r, "isplayed", "isPlayed")),
		IsFavorite:       parseBoolPtr(getCI(r, "isfavorite", "isFavorite")),
		Fields:           splitCSV(getCI(r, "fields")),
		Ids:              splitCSV(getCI(r, "ids")),
		AnyProviderId:    getCI(r, "AnyProviderIdEquals", "anyProviderIdEquals"),
		MinDateLastSaved: parseEmbyTime(getCI(r, "MinDateLastSaved", "minDateLastSaved")),
	}
	if q.StartIndex < 0 {
		q.StartIndex = 0
	}
	if q.Limit < 0 {
		q.Limit = 0
	}
	return q
}

func normalizeSortOrder(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "ascending", "asc":
		return "Ascending"
	case "descending", "desc":
		return "Descending"
	}
	return ""
}

// handleUserItems 对应 GET /emby/Users/{UserId}/Items。
// 响应用 json.Encoder 直接写 Conn，绝不整体缓冲列表。
func (s *Server) handleUserItems(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("UserId")
	// 普通用户不能通过路径换成别人的 UserId 读他人收藏/观看进度；
	// 管理员保留可查任意用户的语义。
	if me := userFrom(r); me != nil && !me.IsAdmin && uid != "" && uid != me.Id {
		uid = me.Id
	}
	s.writeItemsPage(w, r, itemsQueryFrom(r, uid))
}

// handleItems 对应 GET /emby/Items（等价于按 parentId 列出）。
func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	s.writeItemsPage(w, r, itemsQueryFrom(r, ""))
}

func (s *Server) writeItemsPage(w http.ResponseWriter, r *http.Request, q model.ItemsQuery) {
	// 网易爆米花从不请求 /Users/{id}/Views，而是把根 Items（无 ParentId、非递归、不限类型）
	// 当作首页入口。这里直接回媒体库列表（与 /Views 同一个信封），否则它只能看到一堆平铺的
	// 电影/剧集、主页没有媒体库。（VidHub/Moonfin 不受影响。）
	if s.isPopcornClient(r) && r.PathValue("UserId") != "" &&
		q.ParentId == "" && !q.Recursive && len(q.Types) == 0 {
		s.handleViews(w, r)
		return
	}
	// 网易爆米花：对媒体库做递归列表且未指定类型时，只给电影/剧集，
	// 避免把 Season/Episode 平铺出来（它会把 Season 0 等直接当目录显示）。
	if s.isPopcornClient(r) && len(q.Types) == 0 && s.db != nil {
		// 两种情况都裁：ParentId 是媒体库，或客户端直接做全局递归（ParentId 为空）。
		if q.ParentId == "" {
			q.Types = []string{"Movie", "Series"}
		} else if views, err := s.db.GetViews(r.Context()); err == nil {
			for _, v := range views {
				if v.Id == q.ParentId {
					q.Types = []string{"Movie", "Series"}
					break
				}
			}
		}
	}
	if s.db == nil {
		writeJSONStatus(w, http.StatusOK)
		_ = encodePage(w, model.Page{Items: []model.Item{}, StartIndex: q.StartIndex})
		return
	}
	// 普通用户只能访问授权媒体库：带 ParentId 时校验父级归属；
	// 全局/搜索请求把结果限定到授权库，避免通过 /Items 或搜索越权。
	if u := userFrom(r); u != nil {
		if ids, restricted := allowedViewIDs(u); restricted {
			if q.ParentId != "" {
				if !s.itemAllowed(r.Context(), u, q.ParentId) {
					writeError(w, http.StatusForbidden, "library access denied")
					return
				}
			} else {
				if len(ids) == 0 {
					writeJSONStatus(w, http.StatusOK)
					_ = encodePage(w, model.Page{Items: []model.Item{}, StartIndex: q.StartIndex})
					return
				}
				q.AllowedParents = ids
			}
		}
	}
	page, err := s.db.QueryItems(r.Context(), q)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	s.attachListMediaSources(r, page.Items, q.Fields)
	bumpVidHubImageTagsInItems(r, page.Items)
	for i := range page.Items {
		for j := range page.Items[i].MediaSources {
			page.Items[i].MediaSources[j].Path = prettyURL(page.Items[i].MediaSources[j].Path)
		}
		if page.Items[i].Path != "" {
			page.Items[i].Path = prettyURL(page.Items[i].Path)
		}
	}
	writeJSONStatus(w, http.StatusOK)
	_ = encodePage(w, page)
}

// wantsMediaSources 判断列表请求是否需要附带 MediaSources：
// 爆米花（Filmly 等）总是需要（它靠列表里的 MediaSources 判断条目可播/可进详情），
// 其它客户端只在显式请求 Fields=MediaSources 时才附加（保持 VidHub/Moonfin 的响应体不变）。
func (s *Server) wantsMediaSources(r *http.Request, fields []string) bool {
	if s.db == nil {
		return false
	}
	if s.isPopcornClient(r) {
		return true
	}
	for _, f := range fields {
		if strings.EqualFold(strings.TrimSpace(f), "MediaSources") {
			return true
		}
	}
	return false
}

// setPopcornStreamURLs 为爆米花（Filmly 等）的 MediaSources 补 Emby 标准的 DirectStreamUrl。
// 线上现象：Filmly 拿到详情/列表的 MediaSources 后不再请求 PlaybackInfo 的后续播放地址，
// 缺 DirectStreamUrl 时它没有可播地址。其它客户端完全不改（VidHub 的 Path 也不动）。
func (s *Server) setPopcornStreamURLs(r *http.Request, items []model.Item) {
	if (!s.isPopcornClient(r) && !isVidHub(r)) || len(items) == 0 {
		return
	}
	for i := range items {
		fillDirectStreamURLs(r, items[i].MediaSources, items[i].Id)
	}
}

// setPopcornItemStreamURLs 是单条版本（详情页入口用）。
func (s *Server) setPopcornItemStreamURLs(r *http.Request, it *model.Item) {
	if it == nil || (!s.isPopcornClient(r) && !isVidHub(r)) {
		return
	}
	fillDirectStreamURLs(r, it.MediaSources, it.Id)
	// 只对 Filmly 做分辨率实验，VidHub 保留真实分辨率。
	stripFilmlyResolution(r, it.MediaSources)
}

// popcornStreamPrefix 返回爆米花播放地址的前缀（调用方已确认是爆米花客户端）：
//   - Filmly：返回 ""，得到 /Videos/{id}/stream...；Filmly 的 base URL 带 /emby，
//     拼出来就是 /emby/Videos/...，由 Pansave 拦截单前缀并 302 到 CDN（不走服务端代理）。
//   - 其它爆米花：返回 "/emby"，保持 /emby/Videos/...（继续走现有代理/302 策略）。
func popcornStreamPrefix(r *http.Request) string {
	if isFilmly(r) {
		return ""
	}
	return "/emby"
}

// fillDirectStreamURLs 写入 {prefix}/Videos/{id}/stream?Static=true&MediaSourceId={sourceId}。
// Filmly 另外把 MediaSource.Name / Video 流标题换成原始文件名（它只取 Name 的第一个片段，
// “4K · 杜比视界 · 国语 · 粤语” 会显示成 “4K”）；其它客户端完全不变。
func fillDirectStreamURLs(r *http.Request, sources []model.MediaSource, itemID string) {
	prefix := popcornStreamPrefix(r)
	filmly := isFilmly(r)
	for i := range sources {
		if filmly {
			display := filmlyVersionName(sources[i].Name, sources[i].Path)
			sources[i].Name = display
			// Filmly 也可能按流标题展示版本，两条一起给（音轨标题不动）。
			if display != "" {
				for j := range sources[i].MediaStreams {
					if strings.EqualFold(sources[i].MediaStreams[j].Type, "Video") {
						sources[i].MediaStreams[j].Title = display
						sources[i].MediaStreams[j].DisplayTitle = display
					}
				}
			}
		}
		if sources[i].Id == "" {
			continue
		}
		sources[i].DirectStreamUrl = prefix + "/Videos/" + itemID + "/stream?Static=true&MediaSourceId=" + url.QueryEscape(sources[i].Id)
		// VidHub 电视版可能优先使用 Path；把 Path 也换成服务端流地址，
		// 避免它直接访问容器内的 127.0.0.1 直链。
		if isVidHub(r) {
			sources[i].Path = sources[i].DirectStreamUrl
		}
	}
}

// stripFilmlyResolution 把 Filmly 详情/列表里 Video 流的 Width/Height 清零（只动 Video）。
// 线上：手机端 Filmly 的版本下拉仍显示 “4K/1080P”，即使 MediaSource.Name 已经是完整
// 文件名 —— 它很可能是按 MediaStreams 的分辨率生成标签。这里做一轮实验：清掉分辨率后
// 看它是否回退到 Name/DisplayTitle。Codec/VideoRangeType/BitDepth/Title/DisplayTitle 全保留，
// Audio 流一概不动。
// 注意：PlaybackInfo 不调用本函数，播放信息里仍是真实分辨率（客户端据此选片/解码）。
func stripFilmlyResolution(r *http.Request, sources []model.MediaSource) {
	if len(sources) == 0 || !isFilmly(r) {
		return
	}
	for i := range sources {
		for j := range sources[i].MediaStreams {
			st := &sources[i].MediaStreams[j]
			if !strings.EqualFold(st.Type, "Video") {
				continue
			}
			st.Width, st.Height = 0, 0
		}
	}
}

// filmlyVersionName 决定 Filmly 版本下拉里显示的 MediaSource.Name。
// 线上：Filmly 只取 Name 的第一个片段（按 “·”/空格截断），DB 里的
// “4K · 杜比视界 · 国语 · 粤语” 就只剩 “4K”。用户接受直接显示原文件名全名
// （含 2160p/Dovi/HDR10/TrueHD 等规格），所以优先用播放直链里的原始文件名；
// 拿不到文件名、或文件名不比现有标签更有信息量时，保持原 Name（不劣化）。
func filmlyVersionName(name, target string) string {
	fn := mediaFileName(target)
	if fn == "" || fn == name {
		return name
	}
	if hasVersionHint(fn) || len(fn) > len(name) {
		return fn
	}
	return name
}

// versionHintTokens 是“文件名比标签更具体”的判定关键词（小写比较）。
var versionHintTokens = []string{
	"2160p", "1080p", "720p", "480p", "8k", "4k",
	"hdr", "dovi", "dolby", "atmos", "truehd", "dts", "remux",
	"bluray", "blu-ray", "web-dl", "webrip", "hdtv",
	"x264", "x265", "h264", "h265", "hevc", "av1",
}

func hasVersionHint(s string) bool {
	l := strings.ToLower(s)
	for _, tok := range versionHintTokens {
		if strings.Contains(l, tok) {
			return true
		}
	}
	return false
}

// mediaFileName 从播放直链里取原始文件名：优先 file_name 查询参数（云盘直链常见），
// 其次路径 basename；最后去掉媒体扩展名。
func mediaFileName(target string) string {
	raw := strings.TrimSpace(target)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if q, err := url.ParseQuery(u.RawQuery); err == nil {
		if v := strings.TrimSpace(q.Get("file_name")); v != "" {
			// q.Get 已解码一次；部分云盘直链会对文件名二次编码，这里再解一次兜底。
			if dec, err := url.QueryUnescape(v); err == nil && strings.TrimSpace(dec) != "" {
				return trimMediaExt(dec)
			}
			return trimMediaExt(v)
		}
	}
	if u.Path != "" {
		return trimMediaExt(path.Base(u.Path))
	}
	return ""
}

// trimMediaExt 去掉已知的媒体扩展名（“strm” 等非真实容器不动）。
func trimMediaExt(name string) string {
	name = strings.TrimSpace(name)
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		if ext := strings.ToLower(name[i+1:]); videoContainers[ext] {
			return strings.TrimSpace(name[:i])
		}
	}
	return name
}

// attachListMediaSources 为列表批量补 MediaSources（只在客户端需要时），
// 并把来源 Path 转成可读形式（与列表/详情一致，避免中文直链显示成 %E5...）。
func (s *Server) attachListMediaSources(r *http.Request, items []model.Item, fields []string) {
	if !s.wantsMediaSources(r, fields) {
		return
	}
	if err := s.db.AttachMediaSources(r.Context(), items); err != nil {
		log.Printf("列表附加 MediaSources 失败：%v", err)
	}
	for i := range items {
		s.enrichItemSubtitles(r.Context(), &items[i], false)
		for j := range items[i].MediaSources {
			items[i].MediaSources[j].Path = prettyURL(items[i].MediaSources[j].Path)
		}
	}
	// 列表（含 Latest/Resume/Episodes）对爆米花同样要带 DirectStreamUrl。
	s.setPopcornStreamURLs(r, items)
	// 手机端 Filmly 的分辨率实验：列表里也只清 Video 流的分辨率（PlaybackInfo 不动）。
	for i := range items {
		stripFilmlyResolution(r, items[i].MediaSources)
	}
}

// encodePage 流式编码列表信封：{"Items":[...],"TotalRecordCount":N,"StartIndex":S}。
// 直接调用 Encoder.Encode 而不再拷贝 Items 切片。
func encodePage(w http.ResponseWriter, page model.Page) error {
	if page.Items == nil {
		page.Items = []model.Item{}
	}
	return json.NewEncoder(w).Encode(page)
}

// handleItem 对应 GET /emby/Items/{Id}。返回单个条目（含 MediaSources）。
func (s *Server) handleItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	it, err := s.db.GetItem(r.Context(), id)
	if err != nil || it == nil {
		if err != nil && !isNotFound(err) {
			writeError(w, http.StatusInternalServerError, "cannot load item")
			return
		}
		// 媒体库本身也是条目：客户端点库封面时会按 Id 取详情（这里没有 UserId）。
		if s.writeViewDetail(w, r, id) {
			return
		}
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), it.Id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	for i := range it.MediaSources {
		it.MediaSources[i].Path = prettyURL(it.MediaSources[i].Path)
	}
	it.Path = prettyURL(it.Path)
	// 详情页（部分客户端走 /Items/{Id}）也要有外挂/内嵌字幕、音轨和版本大小。
	s.enrichItemSubtitles(r.Context(), it, true)
	// 详情必须带 UserData：VidHub 等客户端靠 PlaybackPositionTicks 决定从哪续播。
	s.setPopcornItemStreamURLs(r, it)
	var returned int64
	uid := ""
	if u := userFrom(r); u != nil {
		uid = u.Id
		_ = s.db.AttachUserData(r.Context(), u.Id, it)
		if it.UserData != nil {
			returned = it.UserData.PlaybackPositionTicks
		}
	}
	// 同 handleUserItem：ChildCount/RecursiveItemCount/UserData 缺失会让 Filmly 退回首页。
	s.ensureDetailShape(r, it)
	bumpVidHubImageTags(r, it)
	s.trace.add(map[string]any{
		"ts": time.Now().UnixMilli(), "path": "/itemDetail2",
		"item": it.Name, "user": uid, "returned": returned,
	})
	writeJSON(w, http.StatusOK, it)
}

// handleUpdateItem 对应 POST /emby/Items/{Id}：手动编辑元数据。
// 提供 LockedFields 后，自动刮削将跳过该条目（与 Emby 的锁定语义一致）。
func (s *Server) handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("Id")
	var body struct {
		Name           *string           `json:"Name"`
		Overview       *string           `json:"Overview"`
		OfficialRating *string           `json:"OfficialRating"`
		ProductionYear *int              `json:"ProductionYear"`
		Genres         []string          `json:"Genres"`
		Tags           []string          `json:"Tags"`
		ProviderIds    map[string]string `json:"ProviderIds"`
		LockedFields   []string          `json:"LockedFields"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	edit := store.ItemEdit{
		Name:           body.Name,
		Overview:       body.Overview,
		OfficialRating: body.OfficialRating,
		ProductionYear: body.ProductionYear,
		Genres:         body.Genres,
		Tags:           body.Tags,
		ProviderIds:    body.ProviderIds,
		Locked:         body.LockedFields,
	}
	if err := s.db.UpdateItemMetadata(r.Context(), id, edit); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot update item")
		return
	}
	it, err := s.db.GetItem(r.Context(), id)
	if err != nil || it == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	writeJSON(w, http.StatusOK, it)
}

// handleResume 对应 GET /emby/Users/{UserId}/Items/Resume。
func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	limit := parseIntDefault(getCI(r, "Limit"), 20)
	items, err := s.db.ResumeItems(r.Context(), u.Id, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load resume items")
		return
	}
	items = s.filterAllowedItems(r.Context(), u, items)
	s.attachListMediaSources(r, items, splitCSV(getCI(r, "Fields")))
	writeJSON(w, http.StatusOK, model.Page{Items: items, TotalRecordCount: len(items), StartIndex: 0})
}

// handleLatest 对应 GET /emby/Users/{UserId}/Items/Latest。
type latestCacheEntry struct {
	at   time.Time
	body []byte
}

// latestVM 是首页“最新”的轻量条目：客户端首屏只需要这些字段。
// 完整字段（Overview/People/MediaSources/ProviderIds 等）在详情页按需取。
type latestVM struct {
	Id                string            `json:"Id"`
	Name              string            `json:"Name"`
	Type              string            `json:"Type"`
	ParentId          string            `json:"ParentId,omitempty"`
	SeriesId          string            `json:"SeriesId,omitempty"`
	SeriesName        string            `json:"SeriesName,omitempty"`
	SeasonId          string            `json:"SeasonId,omitempty"`
	IndexNumber       int               `json:"IndexNumber,omitempty"`
	ParentIndexNumber int               `json:"ParentIndexNumber,omitempty"`
	ProductionYear    int               `json:"ProductionYear,omitempty"`
	PremiereDate      string            `json:"PremiereDate,omitempty"`
	RunTimeTicks      int64             `json:"RunTimeTicks,omitempty"`
	IsFolder          bool              `json:"IsFolder"`
	ImageTags         map[string]string `json:"ImageTags,omitempty"`
	BackdropImageTags []string          `json:"BackdropImageTags,omitempty"`
	UserData          *model.UserData   `json:"UserData,omitempty"`
}

func latestLight(items []model.Item) []latestVM {
	out := make([]latestVM, 0, len(items))
	for i := range items {
		it := &items[i]
		out = append(out, latestVM{
			Id: it.Id, Name: it.Name, Type: it.Type, ParentId: it.ParentId,
			SeriesId: it.SeriesId, SeriesName: it.SeriesName, SeasonId: it.SeasonId,
			IndexNumber: it.IndexNumber, ParentIndexNumber: it.ParentIndexNumber,
			ProductionYear: it.ProductionYear, PremiereDate: it.PremiereDate,
			RunTimeTicks: it.RunTimeTicks, IsFolder: it.IsFolder,
			ImageTags: it.ImageTags, BackdropImageTags: it.BackdropImageTags,
			UserData: it.UserData,
		})
	}
	return out
}

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, http.StatusOK, []model.Item{})
		return
	}
	limit := parseIntDefault(getCI(r, "Limit", "limit"), 20)
	parent := getCI(r, "ParentId", "parentId")
	userID := ""
	u := userFrom(r)
	if u != nil {
		userID = u.Id
		if parent != "" && !s.itemAllowed(r.Context(), u, parent) {
			writeJSON(w, http.StatusOK, []model.Item{})
			return
		}
	}

	moonfin := isMoonfin(r)
	var key string
	if moonfin {
		key = userID + "|" + parent + "|" + strconv.Itoa(limit)
		s.latestMu.Lock()
		if e, ok := s.latestCache[key]; ok && time.Since(e.at) < 10*time.Second {
			body := e.body
			s.latestMu.Unlock()
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Set("Cache-Control", "private, max-age=10")
			_, _ = w.Write(body)
			return
		}
		s.latestMu.Unlock()
	}

	items, err := s.db.LatestItems(r.Context(), parent, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load latest items")
		return
	}
	if u != nil {
		items = s.filterAllowedItems(r.Context(), u, items)
	}
	if userID != "" {
		_ = s.db.AttachUserDataForList(r.Context(), userID, items)
	}
	// Moonfin 用轻量分支（latestLight），不要在那里附加 MediaSources/完整字段。
	if !moonfin {
		s.attachListMediaSources(r, items, splitCSV(getCI(r, "Fields")))
	}

	var payload any = items
	if moonfin {
		payload = latestLight(items)
	}
	body, merr := json.Marshal(payload)
	if merr != nil {
		writeError(w, http.StatusInternalServerError, "cannot encode latest items")
		return
	}
	if moonfin {
		s.latestMu.Lock()
		if s.latestCache == nil {
			s.latestCache = make(map[string]latestCacheEntry, 16)
		}
		if len(s.latestCache) > 64 {
			s.latestCache = make(map[string]latestCacheEntry, 16)
		}
		s.latestCache[key] = latestCacheEntry{at: time.Now(), body: body}
		s.latestMu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if moonfin {
		w.Header().Set("Cache-Control", "private, max-age=10")
	}
	_, _ = w.Write(body)
}

// itemRefreshTimeout 是“没有任务队列时”同步兜底刮削的时间上限。
const itemRefreshTimeout = 30 * time.Second

// handleItemRefresh 对应 POST /emby/Items/{Id}/Refresh：标记待重新刮削并尽快重刮。
// 刷新一律进任务队列（受 heavyMu / 并发上限 / 可取消约束），绝不在这里裸起 goroutine：
// 客户端打开详情页会连续刷新，每次一个不可取消的 goroutine 会绕过队列把写库串成一片，
// 服务端随即被拖慢甚至卡死（Web 管理页跟着转圈）。
func (s *Server) handleItemRefresh(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := r.PathValue("Id")
	if !s.itemAllowed(r.Context(), u, id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if err := s.db.TouchItem(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot refresh item")
		return
	}
	name := id
	if row, err := s.db.ItemRow(r.Context(), id); err == nil && row != nil && row.Name != "" {
		name = row.Name
	}
	// 首选任务队列：任务中心可见、有进度、可取消、与扫描/刮削互斥。
	if s.cfg.EnqueueItemJob != nil && s.cfg.EnqueueItemJob("rescore", id, name) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.cfg.RescoreItem == nil {
		// 既没有队列也没有刮削器：明确告知客户端，而不是假装成功。
		writeError(w, http.StatusServiceUnavailable, "rescore job queue is not ready")
		return
	}
	// 兜底：同步执行，但必须有超时上限；跟随请求 context，客户端断开即可取消。
	// 失败只记日志不再让客户端重试 —— TouchItem 已把条目标记为待刮削，
	// 下一次队列/全量刮削会补上。
	ctx, cancel := context.WithTimeout(r.Context(), itemRefreshTimeout)
	defer cancel()
	if err := s.cfg.RescoreItem(ctx, id); err != nil {
		log.Printf("单条刷新刮削失败 %s：%v", id, err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseIntDefault 解析整型查询参数，失败时返回默认值。
func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return def
	}
	return v
}

// parseEmbyTime 解析 Emby 的时间参数（RFC3339 或 unix 秒）；失败返回 0。
func parseEmbyTime(v string) int64 {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.0000000Z"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.Unix()
		}
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	return 0
}

// handleUserItemData 对应 GET/POST /emby/Users/{UserId}/Items/{ItemId}/UserData。
// 标准 Emby 客户端（含网易爆米花）用它读取/写回播放进度，body 里的 PlaybackPositionTicks 是权威值。
func (s *Server) handleUserItemData(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	itemID := r.PathValue("ItemId")
	it, err := s.db.GetItem(r.Context(), itemID)
	if err != nil || it == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if !s.itemAllowed(r.Context(), u, it.Id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	if r.Method == http.MethodPost {
		var body struct {
			PlaybackPositionTicks int64 `json:"PlaybackPositionTicks"`
			Played                *bool `json:"Played"`
		}
		if err := decodeJSONBody(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
		pos := body.PlaybackPositionTicks
		played := false
		if body.Played != nil {
			played = *body.Played
		} else if it.RunTimeTicks > 0 {
			played = float64(pos)/float64(it.RunTimeTicks) >= 0.9
		}
		now := time.Now()
		if err := s.db.UpsertPlaybacks(r.Context(), []store.PlaybackProgress{{
			UserID: u.Id, ItemID: it.Id, PositionTicks: pos, DurationTicks: it.RunTimeTicks,
			Played: played, LastPlayed: now.Unix(), Updated: now.UnixMilli(),
		}}); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot save user data")
			return
		}
	}
	if err := s.db.AttachUserData(r.Context(), u.Id, it); err != nil || it.UserData == nil {
		writeJSON(w, http.StatusOK, map[string]any{"PlaybackPositionTicks": 0, "Played": false})
		return
	}
	writeJSON(w, http.StatusOK, it.UserData)
}
