package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"emby-air/internal/model"
	"emby-air/internal/scanner"
)

// ---- 杂项兼容端点：形状对齐 Emby，数据尽量真实，做不到的返回空集合。 ----

func (s *Server) handleSimilar(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
}

func (s *Server) handleEmptyPage(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
}

func (s *Server) handleEmptyList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, []any{})
}

func (s *Server) handleNoContent(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleItemsCounts(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{
		"MovieCount": 0, "SeriesCount": 0, "EpisodeCount": 0, "ArtistCount": 0,
		"ProgramCount": 0, "TrailerCount": 0, "SongCount": 0, "AlbumCount": 0,
		"MusicVideoCount": 0, "BoxSetCount": 0, "BookCount": 0, "ItemCount": 0,
	}
	if s.db != nil {
		if st, err := s.db.Stats(r.Context()); err == nil {
			out["MovieCount"] = st.Movies
			out["SeriesCount"] = st.Series
			out["EpisodeCount"] = st.Episodes
			out["ItemCount"] = st.Items
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// handleAuthKeys 对应 GET/POST/DELETE /emby/Auth/Keys。
func (s *Server) handleAuthKeys(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Items": []any{}, "TotalRecordCount": 0})
		return
	}
	switch r.Method {
	case http.MethodGet:
		keys, err := s.db.ListAPIKeys(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot list keys")
			return
		}
		items := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			items = append(items, map[string]any{
				"Id": k.Key, "AccessToken": k.Key, "AppName": k.Note,
				"DateCreated": k.Created, "DateLastActivity": k.Created,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"Items": items, "TotalRecordCount": len(items)})
	case http.MethodPost:
		key, err := s.db.CreateAPIKey(r.Context(), "api")
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot create key")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"Id": key, "AccessToken": key, "AppName": "api"})
	case http.MethodDelete:
		key := firstNonEmpty(r.URL.Query().Get("Key"), r.URL.Query().Get("key"), r.URL.Query().Get("Id"))
		if key != "" {
			_ = s.db.DeleteAPIKey(r.Context(), key)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleUserItemState 处理收藏 / 已看标记：
//
//	POST/DELETE /emby/Users/{UserId}/FavoriteItems/{ItemId}
//	POST/DELETE /emby/Users/{UserId}/PlayedItems/{ItemId}
func (s *Server) handleUserItemState(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	itemID := r.PathValue("ItemId")
	if !s.itemAllowed(r.Context(), u, itemID) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	fav := strings.Contains(strings.ToLower(r.URL.Path), "favorite")
	on := r.Method == http.MethodPost
	if err := s.db.SetUserItemFlag(r.Context(), u.Id, itemID, fav, on); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot update user data")
		return
	}
	if it, err := s.db.GetItem(r.Context(), itemID); err == nil && it != nil {
		writeJSON(w, http.StatusOK, it)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleCompatFallback 兜住所有未实现的 /emby/* 路径。
// 目的：第三方工具探测冷门端点时不至于被 404 打断；同时把路径记进日志便于补齐。
func (s *Server) handleCompatFallback(w http.ResponseWriter, r *http.Request) {
	if mw := s.compatLogger(); mw != nil {
		mw(r)
	}
	switch r.Method {
	case http.MethodGet:
		// /emby/Library/* 这类“列表型”端点返回数组，其余返回空对象。
		if strings.Contains(strings.ToLower(r.URL.Path), "/library/") ||
			strings.HasSuffix(strings.ToLower(r.URL.Path), "/query") {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{})
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// compatLogger 由中间件注入（可选），用于记录未实现端点。
func (s *Server) compatLogger() func(*http.Request) { return s.cfg.CompatLog }

func (s *Server) handleEmptyObject(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{})
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// handleUserItem 对应 GET /emby/Users/{UserId}/Items/{ItemId}（VidHub 详情页入口）。
func (s *Server) handleUserItem(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	id := r.PathValue("ItemId")
	it, err := s.db.GetItem(r.Context(), id)
	if err == nil && it != nil {
		if !s.itemAllowed(r.Context(), userFrom(r), it.Id) {
			writeError(w, http.StatusNotFound, "item not found")
			return
		}
		for i := range it.MediaSources {
			it.MediaSources[i].Path = prettyURL(it.MediaSources[i].Path)
		}
		it.Path = prettyURL(it.Path)
		// 爆米花/Filmly 的详情页只认 MediaSources 里的 DirectStreamUrl（与 PlaybackInfo 一致）。
		s.enrichItemSubtitles(r.Context(), it, true)
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
		// ChildCount/RecursiveItemCount/UserData 必须齐：Filmly 缺字段会直接回首页。
		s.ensureDetailShape(r, it)
		s.trace.add(map[string]any{
			"ts": time.Now().UnixMilli(), "path": "/itemDetail",
			"item": it.Name, "user": uid, "returned": returned,
		})
		writeJSON(w, http.StatusOK, it)
		return
	}
	// 媒体库本身也是个“条目”：客户端点库时会按 Id 取详情。
	if s.writeViewDetail(w, r, id) {
		return
	}
	writeError(w, http.StatusNotFound, "item not found")
}

// ensureDetailShape 保证详情响应带齐客户端用来“判定能不能进”的标准字段：
//   - ChildCount / RecursiveItemCount：结构体零值即可（json tag 已去掉 omitempty，0 也会下发）。
//     Series 仍用真实集数（客户端靠它显示“共 N 集”）；Movie/Episode/Season 明确置 0。
//   - UserData：没有观看记录时 AttachUserData 不会赋值，这里补一个空对象，
//     否则 Filmly 可能因字段缺失而解析/初始化失败、直接退回首页。
func (s *Server) ensureDetailShape(r *http.Request, it *model.Item) {
	if it == nil {
		return
	}
	switch it.Type {
	case "Series":
		if s.db != nil {
			if n, e := s.db.CountEpisodes(r.Context(), it.Id); e == nil {
				it.ChildCount = n
				it.RecursiveItemCount = n
			}
		}
	case "Movie", "Episode", "Season":
		it.ChildCount = 0
		it.RecursiveItemCount = 0
	}
	if it.UserData == nil {
		it.UserData = &model.UserData{ItemId: it.Id}
	}
}

// writeViewDetail 在 items 表里查不到 id 时按媒体库处理：命中则写响应并返回 true。
// 爆米花（Filmly 等）点库封面拿到的详情必须是 CollectionFolder：线上它在详情上下文中
// 不接受 UserView（提示“未知文件类型”并回首页），而 /Views 入口仍是 UserView。
// 其它客户端（VidHub/Moonfin）保持原来的 CollectionFolder，行为不变。
func (s *Server) writeViewDetail(w http.ResponseWriter, r *http.Request, id string) bool {
	if s.db == nil || id == "" {
		return false
	}
	if !s.viewAllowed(r.Context(), userFrom(r), id) {
		return false
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		return false
	}
	for _, v := range views {
		if v.Id != id {
			continue
		}
		if s.isPopcornClient(r) {
			writeJSON(w, http.StatusOK, s.popcornViewDetailJSON(r, v))
			return true
		}
		vi := viewToItem(v, s.serverID())
		// 非爆米花客户端也补上 ChildCount：Emby 的 CollectionFolder 本来就有该字段，
		// 缺字段会让部分客户端把库当成空目录（VidHub 的 Type 仍是 CollectionFolder，不变）。
		if s.db != nil {
			vi.ChildCount = s.db.ViewChildCount(r.Context(), v.Id)
			vi.RecursiveItemCount = vi.ChildCount
		}
		cover := filepath.Join(s.cfg.ImageCache, "libraries", v.Id+".img")
		if _, ok := readFileLimited(cover); ok {
			vi.ImageTags = map[string]string{"Primary": imageVersionTagOf(cover)}
		}
		writeJSON(w, http.StatusOK, vi)
		return true
	}
	return false
}

// handleShowSeasons 对应 GET /emby/Shows/{Id}/Seasons。
func (s *Server) handleShowSeasons(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	if s.db == nil {
		writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	items, err := s.db.Children(r.Context(), id, "Season")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list seasons")
		return
	}
	// 季列表也要带 UserData：客户端会把这里返回的对象缓存下来当作详情页数据源。
	if u := userFrom(r); u != nil {
		_ = s.db.AttachUserDataForList(r.Context(), u.Id, items)
	}
	// 爆米花（Filmly）对 null 敏感：季条目的 UserData 不能为 null，ChildCount 也要有真实集数，
	// 否则它在“剧集 → 季 → 集”的链路上判定数据不完整，报“尝试连接时发生错误”。
	if s.isPopcornClient(r) {
		s.fixPopcornSeasons(r, id, items)
	}
	writeJSON(w, http.StatusOK, model.Page{Items: items, TotalRecordCount: len(items)})
}

// fixPopcornSeasons 给爆米花补齐季条目的 UserData、集数统计，并把英文季名本地化。
// 季数量很少，这里只做一次全量集查询再按季号聚合（不逐季查库）。
func (s *Server) fixPopcornSeasons(r *http.Request, seriesID string, items []model.Item) {
	if s.db == nil || len(items) == 0 {
		return
	}
	counts := map[int]int{}
	if eps, err := s.db.SeriesEpisodes(r.Context(), seriesID, -1); err == nil {
		for i := range eps {
			counts[eps[i].ParentIndexNumber]++
		}
	}
	for i := range items {
		if items[i].UserData == nil {
			items[i].UserData = &model.UserData{ItemId: items[i].Id}
		}
		n := counts[items[i].ParentIndexNumber]
		items[i].ChildCount = n
		items[i].RecursiveItemCount = n
		// 线上：爆米花把 Season.Name 直接拼在剧名后面，剧集详情显示成“剧名 Season 1”。
		// 只改 "Season N" 这种纯英文格式，其它命名（含已经本地化的）原样保留。
		items[i].Name = popcornSeasonName(items[i].Name)
	}
}

// popcornSeasonName 把爆米花用的英文季名本地化："Season 1" → "第 1 季"，"Season 0" → "特别篇"。
// 不是 "Season <数字>" 格式的一律原样返回（IndexNumber 不受影响）。
func popcornSeasonName(name string) string {
	const prefix = "Season "
	if !strings.HasPrefix(name, prefix) {
		return name
	}
	n, err := strconv.Atoi(strings.TrimSpace(name[len(prefix):]))
	if err != nil || n < 0 {
		return name
	}
	if n == 0 {
		return "特别篇"
	}
	return "第 " + strconv.Itoa(n) + " 季"
}

// fillEmptyUserData 给列表条目补非 null 的 UserData（未观看时 AttachUserDataForList
// 不会赋值，直接下发 null 会让爆米花/Filmly 中断整条链路）。
func fillEmptyUserData(items []model.Item) {
	for i := range items {
		if items[i].UserData == nil {
			items[i].UserData = &model.UserData{ItemId: items[i].Id}
		}
	}
}

// handleShowEpisodes 对应 GET /emby/Shows/{Id}/Episodes。
func (s *Server) handleShowEpisodes(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("Id")
	if s.db == nil {
		writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
		return
	}
	if !s.itemAllowed(r.Context(), userFrom(r), id) {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}
	season := parseIntDefault(getCI(r, "Season"), -1)
	// VidHub 等客户端用 ?SeasonId=<季条目ID>，不传 Season 数字。
	if sid := getCI(r, "SeasonId", "seasonId"); sid != "" {
		if n, ok := s.db.SeasonNumber(r.Context(), sid); ok {
			season = n
		}
	}
	items, err := s.db.SeriesEpisodes(r.Context(), id, season)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list episodes")
		return
	}
	// ★ 关键：剧集列表是 VidHub 详情页的数据源，缺 UserData 时它认为“没看过”，
	// 于是每次点播放都从头开始（电影走媒体库列表，那边有 UserData，所以正常）。
	if u := userFrom(r); u != nil {
		_ = s.db.AttachUserDataForList(r.Context(), u.Id, items)
	}
	// 爆米花/Filmly 会在剧集列表里直接判断可否播放；Fields=MediaSources 时其它客户端同理。
	s.attachListMediaSources(r, items, splitCSV(getCI(r, "Fields")))
	// 爆米花列表里未看过的条目 UserData 为 null 会被当成数据异常，补成零值对象。
	if s.isPopcornClient(r) {
		fillEmptyUserData(items)
	}
	writeJSON(w, http.StatusOK, model.Page{Items: items, TotalRecordCount: len(items)})
}

// handleNextUp 对应 GET /emby/Shows/NextUp：
// 返回“下一集”（优先未看完的那一集），客户端用它做剧集的继续播放。
func (s *Server) handleNextUp(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
		return
	}
	limit := parseIntDefault(getCI(r, "Limit"), 20)
	seriesID := getCI(r, "SeriesId", "seriesId")
	if seriesID != "" && !s.itemAllowed(r.Context(), u, seriesID) {
		writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0})
		return
	}
	items, err := s.db.NextUpEpisodes(r.Context(), u.Id, seriesID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot load next up")
		return
	}
	items = s.filterAllowedItems(r.Context(), u, items)
	if err := s.db.AttachUserDataForList(r.Context(), u.Id, items); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot attach user data")
		return
	}
	// 同剧集列表：爆米花的 NextUp 条目也不能带 null UserData。
	if s.isPopcornClient(r) {
		fillEmptyUserData(items)
	}
	writeJSON(w, http.StatusOK, model.Page{Items: items, TotalRecordCount: len(items)})
}

// handlePublicUsers 对应 GET /emby/Users/Public：无需认证的用户列表（只给最小字段）。
func (s *Server) handlePublicUsers(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	// 管理员可在 Web「用户」页关闭登录页用户列表（默认开启，保持原行为）。
	if s.db.GetSetting(r.Context(), publicUsersVisibleKey) == "0" {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	users, err := s.db.ListUsers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list users")
		return
	}
	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, map[string]any{
			"Id": u.Id, "Name": u.Name, "HasPassword": u.HasPassword,
			"PrimaryImageTag": u.PrimaryImageTag,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleShowsMissing 对应 GET /emby/Shows/Missing：
// 以 TMDB 的季集数为基准，列出本地缺失的分集（真实统计，不是空占位）。
func (s *Server) handleShowsMissing(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ShowMissing == nil || s.db == nil {
		writeJSON(w, http.StatusOK, map[string]any{"Items": []any{}, "TotalRecordCount": 0, "StartIndex": 0})
		return
	}
	start := parseIntDefault(getCI(r, "StartIndex", "startIndex"), 0)
	limit := parseIntDefault(getCI(r, "Limit", "limit"), 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	includeSpecials := boolValue(getCI(r, "IncludeSpecials", "includeSpecials"))
	includeUnaired := boolValue(getCI(r, "IncludeUnaired", "includeUnaired"))
	parent := getCI(r, "ParentId", "parentId")

	var seriesIDs []string
	if parent != "" {
		if !s.itemAllowed(r.Context(), userFrom(r), parent) {
			writeJSON(w, http.StatusOK, map[string]any{"Items": []any{}, "TotalRecordCount": 0, "StartIndex": 0})
			return
		}
		seriesIDs = []string{parent}
	} else {
		// 不指定剧集时，最多查 20 部（每部 1 次 TMDB 请求），避免一次打爆接口。
		ids, err := s.db.ListSeriesIDs(r.Context(), 20, 0)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot list series")
			return
		}
		if u := userFrom(r); u != nil {
			for _, id := range ids {
				if s.itemAllowed(r.Context(), u, id) {
					seriesIDs = append(seriesIDs, id)
				}
			}
		} else {
			seriesIDs = ids
		}
	}

	all := make([]model.Item, 0, 64)
	for _, sid := range seriesIDs {
		items, err := s.cfg.ShowMissing(r.Context(), sid, includeSpecials, includeUnaired)
		if err != nil {
			continue // 单部查询失败不影响其它剧
		}
		all = append(all, items...)
	}
	total := len(all)
	if start > total {
		start = total
	}
	end := total
	if start+limit < end {
		end = start + limit
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"Items": all[start:end], "TotalRecordCount": total, "StartIndex": start,
	})
}

// handleMediaUpdated 对应 POST /emby/Library/Media/Updated（Pansave 通知文件变化）。
// Pansave 会给出发生变化的标题目录（/media/...），优先把这些目录作为独立任务
// 放进任务队列，而不是整库重扫。没有可解析路径时保留旧的整库刷新兜底。
// 返回语义保持不变：无论如何都回 204（Pansave 只是“通知”，不关心扫描是否已在跑）。
func (s *Server) handleMediaUpdated(w http.ResponseWriter, r *http.Request) {
	paths, entries := s.mediaUpdatedPaths(r)
	if before := len(paths); before > 0 {
		paths = s.filterMediaPathsToViews(r.Context(), paths)
		if len(paths) < before {
			log.Printf("媒体变更通知：忽略 %d 个不在已选媒体库路径下的目录", before-len(paths))
		}
	}
	if len(paths) > 0 && s.cfg.EnqueuePaths != nil {
		name := "Pansave 定向刷新（" + strconv.Itoa(len(paths)) + " 个目录）"
		log.Printf("媒体变更通知：收到 %d 条更新，定向扫描 %d 个目录", entries, len(paths))
		if !s.cfg.EnqueuePaths(paths, name) {
			log.Printf("媒体变更通知：任务队列拒绝，改为整库刷新")
		} else {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if s.cfg.RefreshAll != nil {
		if !s.cfg.RefreshAll() {
			log.Printf("媒体变更通知：已有扫描在跑，忽略本次触发")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.StartScan(s.cfg.MediaRoot)
	w.WriteHeader(http.StatusNoContent)
}

// filterMediaPathsToViews 只保留配置媒体库路径内的目录。
// 防止历史 bug 或 Pansave 通知把 /media 下未选中的目录（如人工核实区）带进索引。
func (s *Server) filterMediaPathsToViews(ctx context.Context, paths []string) []string {
	if s.db == nil || len(paths) == 0 {
		return nil
	}
	views, err := s.db.GetViews(ctx)
	if err != nil {
		return nil
	}
	var roots []string
	for _, v := range views {
		for _, p := range v.Paths {
			if p = strings.TrimSpace(p); p != "" {
				roots = append(roots, filepath.Clean(p))
			}
		}
	}
	within := func(p string) bool {
		p = filepath.Clean(p)
		for _, r := range roots {
			if p == r || strings.HasPrefix(p, r+string(filepath.Separator)) {
				return true
			}
		}
		return false
	}
	out := paths[:0]
	for _, p := range paths {
		if within(p) {
			out = append(out, p)
		}
	}
	return out
}

type mediaUpdate struct {
	Path       string `json:"Path"`
	UpdateType string `json:"UpdateType"`
}

// mediaUpdatedPaths 读取 Emby/Pansave 的 Updates 数组，换算成媒体根内的标题目录。
// 解析失败或没有路径时返回空，由调用方走整库刷新。
func (s *Server) mediaUpdatedPaths(r *http.Request) ([]string, int) {
	if s.cfg.MediaRoot == "" {
		return nil, 0
	}
	var body struct {
		Updates []mediaUpdate `json:"Updates"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil && err != io.EOF {
		log.Printf("媒体变更通知：解析失败，改为整库刷新：%v", err)
		return nil, 0
	}
	return normalizeMediaUpdatePaths(s.cfg.MediaRoot, body.Updates), len(body.Updates)
}

// normalizeMediaUpdatePaths 只保留媒体根内的路径，并用标题目录去重、排序。
// Pansave 传来的可能是 /media/xxx/Title (2026)/file.strm，也可能是目录本身；
// TitleDir 对已删除文件也能按扩展名推断父目录，因此删除通知同样能落到标题目录。
func normalizeMediaUpdatePaths(mediaRoot string, updates []mediaUpdate) []string {
	root := filepath.Clean(mediaRoot)
	if root == "." || root == "" {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(updates))
	for _, u := range updates {
		p := strings.TrimSpace(u.Path)
		if p == "" || !filepath.IsAbs(p) {
			continue
		}
		clean := filepath.Clean(p)
		rel, err := filepath.Rel(root, clean)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		title := scanner.TitleDir(clean, root)
		if title == "" {
			title = clean
		}
		title = filepath.Clean(title)
		if title != root {
			rel, err = filepath.Rel(root, title)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				continue
			}
		}
		if !seen[title] {
			seen[title] = true
			out = append(out, title)
		}
	}
	sort.Strings(out)
	return out
}

// handleVideoAlias 处理 /Videos/{Id}/stream.mkv、/Videos/{Id}/original 等变体。
func (s *Server) handleVideoAlias(w http.ResponseWriter, r *http.Request) {
	action := strings.ToLower(r.PathValue("Action"))
	if strings.HasPrefix(action, "stream") || strings.HasPrefix(action, "original") {
		s.handleVideoStream(w, r)
		return
	}
	writeError(w, http.StatusNotFound, "unsupported media action")
}
