package httpapi

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"emby-air/internal/model"
	"emby-air/internal/store"
)

// handleUsersMe 返回当前令牌对应的用户。
func (s *Server) handleUsersMe(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	out := *u
	out.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, &out)
}

// handleUserByID 返回指定用户。
// 管理员 / API Key 可以查询任意用户（Pansave 等服务探测会用 API Key 取 /Users/{uid}，
// 只允许“自己”会让探测拿到 404，被客户端当成服务异常）；普通用户仍只能查自己。
func (s *Server) handleUserByID(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	id := r.PathValue("UserId")
	if constTimeEq(id, u.Id) {
		out := *u
		out.ServerId = s.serverID()
		writeJSON(w, http.StatusOK, &out)
		return
	}
	if !u.IsAdmin && !isAPIKeyUser(u) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	target, err := s.db.UserByID(r.Context(), id)
	if err != nil || target == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	out := *target
	out.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, &out)
}

// handleUsers 列出用户：管理员看到全部账号（含 IsAdmin），普通用户只看到自己。
func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !u.IsAdmin || s.db == nil {
		out := *u
		out.ServerId = s.serverID()
		writeJSON(w, http.StatusOK, []model.User{out})
		return
	}
	users, err := s.db.ListUsers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list users")
		return
	}
	for i := range users {
		users[i].ServerId = s.serverID()
	}
	writeJSON(w, http.StatusOK, users)
}

// handleViews 返回用户可见的媒体库。
func (s *Server) handleViews(w http.ResponseWriter, r *http.Request) {
	// 库权限变化后客户端可能缓存旧列表；明确禁止缓存，减少“已取消的库还显示”。
	w.Header().Set("Cache-Control", "no-store")
	if s.db == nil {
		writeJSON(w, http.StatusOK, model.Page{Items: []model.Item{}, TotalRecordCount: 0, StartIndex: 0})
		return
	}
	views, err := s.db.GetViews(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot list views")
		return
	}
	// 按用户策略过滤可见媒体库。
	var policy *model.UserPolicy
	if u := userFrom(r); u != nil {
		policy = u.Policy
	}
	popcorn := s.isPopcornClient(r)
	items := make([]any, 0, len(views))
	for _, v := range views {
		if policy != nil && !policy.EnableAllFolders {
			allowed := false
			for _, f := range policy.EnabledFolders {
				if constTimeEq(f, v.Id) {
					allowed = true
					break
				}
			}
			if !allowed {
				continue
			}
		}
		if popcorn {
			// 爆米花按 Emby 的 UserView 解析媒体库；CollectionFolder 会被它当成普通文件夹。
			items = append(items, s.popcornViewJSON(r, v))
			continue
		}
		it := viewToItem(v, s.serverID())
		// 有封面时给出 ImageTags，否则客户端不会请求库封面。
		if _, ok := readFileLimited(filepath.Join(s.cfg.ImageCache, "libraries", v.Id+".img")); ok {
			it.ImageTags = map[string]string{"Primary": imageVersionTagOf(filepath.Join(s.cfg.ImageCache, "libraries", v.Id+".img"))}
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"Items":            items,
		"TotalRecordCount": len(items),
		"StartIndex":       0,
	})
}

// popcornViewJSON 生成爆米花（Filmly 等）用的媒体库条目：Type=UserView。
// CollectionFolder 会被爆米花当成普通文件夹，点击封面进不了库，所以
// /Views、媒体库详情、/Items/{库Id} 三处都必须返回同一形状（避免两份逻辑漂移）。
func (s *Server) popcornViewJSON(r *http.Request, v model.View) map[string]any {
	tags := map[string]string{}
	cover := filepath.Join(s.cfg.ImageCache, "libraries", v.Id+".img")
	if _, ok := readFileLimited(cover); ok {
		tags["Primary"] = imageVersionTagOf(cover)
	}
	// Filmly 点库封面拿到详情后会用 ChildCount 判断“库里有没有内容”，
	// 缺这两个字段（或为 0）它会认为库是空的，直接回首页。
	childCount := 0
	if s.db != nil {
		childCount = s.db.ViewChildCount(r.Context(), v.Id)
	}
	// ImageTags/UserData 等标准字段不能省：部分版本对缺字段敏感，会整条跳过。
	return map[string]any{
		"Id":                 v.Id,
		"Name":               v.Name,
		"ServerId":           s.serverID(),
		"Type":               "UserView",
		"CollectionType":     v.CollectionType,
		"IsFolder":           true,
		"ChildCount":         childCount,
		"RecursiveItemCount": childCount,
		"ImageTags":          tags,
		"BackdropImageTags":  []string{},
		"UserData":           viewUserDataJSON(v.Id),
	}
}

// popcornViewDetailJSON 生成爆米花媒体库“详情”响应，类型按客户端分流：
//   - Filmly 手机端（UA Filmly/2.12.x）点库封面会请求 /Users/{uid}/Items/{viewId}。
//     线上依次试过 UserView → CollectionFolder（缺 ItemId/完整字段）→ Folder，都被判成
//     “未知文件类型”回首页；本轮按新组合回到 Emby 原生 CollectionFolder，同时补齐 ItemId
//     与下面全部 DTO 字段（CollectionFolder + ItemId + 完整字段）。
//   - 其它爆米花（电脑端 网易爆米花 UA 网易爆米花/76 CFNetwork/...）不走这个入口
//     （它直接请求 /Items?ParentId=...），保持上一轮已验证可用的 Folder，不动它的行为。
//
// /Views 入口仍用 popcornViewJSON(UserView)，其它客户端仍走 viewToItem(CollectionFolder)。
func (s *Server) popcornViewDetailJSON(r *http.Request, v model.View) map[string]any {
	tags := map[string]string{}
	cover := filepath.Join(s.cfg.ImageCache, "libraries", v.Id+".img")
	if _, ok := readFileLimited(cover); ok {
		tags["Primary"] = imageVersionTagOf(cover)
	}
	childCount := 0
	if s.db != nil {
		childCount = s.db.ViewChildCount(r.Context(), v.Id)
	}
	// 媒体库路径：取第一个登记路径（多路径库也只给第一个，够客户端拼“目录”语义）。
	libPath := ""
	if len(v.Paths) > 0 {
		libPath = strings.TrimSpace(v.Paths[0])
	}
	// Filmly 手机端用 CollectionFolder（配 ItemId/完整字段）；其它爆米花保持 Folder，
	// 不改变电脑端已确认可用的路径。
	viewType := "Folder"
	if isFilmly(r) {
		viewType = "CollectionFolder"
	}
	out := map[string]any{
		"Id":       v.Id,
		"ItemId":   v.Id, // 部分客户端用 ItemId 而非 Id 定位条目，两个都给
		"Name":     v.Name,
		"SortName": v.Name,
		// ForcedSortName 在 Emby 里是“用户强制排序名”，媒体库没有，与 Name 一致最安全。
		"ForcedSortName":     v.Name,
		"ServerId":           s.serverID(),
		"Type":               viewType,
		"CollectionType":     v.CollectionType,
		"IsFolder":           true,
		"ChildCount":         childCount,
		"RecursiveItemCount": childCount,
		// ImageTags/UserData 等标准字段不能省：部分版本对缺字段敏感，会整条跳过。
		"ImageTags":         tags,
		"BackdropImageTags": []any{},
		// UserData 不能是 null（Filmly 对 null 敏感），媒体库本身没有观看状态，字段取零值。
		"UserData": viewUserDataJSON(v.Id),
		// 空白切片/对象一律显式给空值，不能是 nil（否则序列化成 null，客户端解析失败）。
		"ExternalUrls":     []any{},
		"Taglines":         []any{},
		"Genres":           []any{},
		"Studios":          []any{},
		"People":           []any{},
		"ProviderIds":      map[string]string{},
		"Path":             libPath,
		"LocationType":     "FileSystem",
		"ParentId":         "",
		"MediaType":        "Unknown",
		"CanDelete":        false,
		"CanDownload":      false,
		"PlayAccess":       "Full",
		"IsPlayable":       false,
		"IsMissing":        false,
		"IsVirtualUnaired": false,
		// 媒体库没有 MediaSources，但仍要声明“可显示来源区”，避免客户端按字段缺失判定不可播。
		"EnableMediaSourceDisplay": true,
		"PrimaryImageAspectRatio":  0.6667, // 媒体库封面按海报比例；固定值避免客户端布局抖动
	}
	// DateCreated 可选：用媒体库目录的修改时间（Emby 的时间格式）；路径不存在时干脆不下发，
	// 不下发非法日期比下发空串安全。
	if created := pathDateCreated(libPath); created != "" {
		out["DateCreated"] = created
	}
	return out
}

// pathDateCreated 返回路径的修改时间（Emby 的 DateCreated 格式）；路径不存在返回空串。
func pathDateCreated(p string) string {
	if p == "" {
		return ""
	}
	fi, err := os.Stat(p)
	if err != nil {
		return ""
	}
	return fi.ModTime().UTC().Format("2006-01-02T15:04:05.0000000Z")
}

// viewUserDataJSON 是媒体库条目用的 UserData：Emby 的 UserItemDataDto 恒带 ItemId，
// 缺失或 null 都会被客户端当成解析失败，所以这里给零值对象而不是 null。
func viewUserDataJSON(id string) map[string]any {
	return map[string]any{
		"ItemId": id, "Played": false, "PlayCount": 0,
		"PlaybackPositionTicks": 0, "PlayedPercentage": 0, "IsFavorite": false,
	}
}

// viewToItem 把 View 映射成 Emby 的 UserView 条目。
func viewToItem(v model.View, serverID string) model.Item {
	t := v.Type
	if t == "" {
		t = "CollectionFolder"
	}
	return model.Item{
		Id:             v.Id,
		Name:           v.Name,
		ServerId:       serverID,
		Type:           t,
		CollectionType: v.CollectionType, // 客户端靠它区分 电影/剧集
		IsFolder:       v.IsFolder || t == "CollectionFolder" || t == "UserView",
	}
}

// handleCreateUser 对应 POST /emby/Users/New（管理员）。
// Emby 的语义：body 里带 Name/Password，返回新建的 User。密码为空表示免密账号。
func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	var body struct {
		Name            string `json:"Name"`
		Password        string `json:"Password"`
		IsAdministrator bool   `json:"IsAdministrator"`
		IsAdmin         bool   `json:"IsAdmin"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if existing, _, err := s.db.UserByName(r.Context(), name); err == nil && existing != nil {
		writeError(w, http.StatusBadRequest, "user already exists")
		return
	}
	nu, err := s.db.CreateUser(r.Context(), name, body.Password, body.IsAdministrator || body.IsAdmin)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot create user: "+err.Error())
		return
	}
	nu.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, nu)
}

// handleDeleteUser 对应 DELETE /emby/Users/{UserId}（管理员）。
// 不允许删除最后一个账号，避免把自己锁在门外。
func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if u == nil || !u.IsAdmin {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("UserId")
	if id == "" || id == u.Id {
		writeError(w, http.StatusBadRequest, "cannot delete the current user")
		return
	}
	n, err := s.db.DeleteUser(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot delete user")
		return
	}
	s.dropMemTokensForUser(id)
	if n == 0 {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleUpdatePolicy 对应 POST /emby/Users/{UserId}/Policy（管理员）。
// 采用“只更新出现字段”的语义：Web 端只发 IsDisabled 时不会顺手把管理员降级、
// 只发库权限时不会重新启用账号。Emby 客户端发完整 policy 时行为不变。
func (s *Server) handleUpdatePolicy(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || !u.IsAdmin || s.db == nil {
		writeError(w, http.StatusForbidden, "admin required")
		return
	}
	id := r.PathValue("UserId")
	target, err := s.db.UserByID(r.Context(), id)
	if err != nil || target == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	var body store.UserPolicyPatch
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}

	oldPolicy := target.Policy
	oldAdmin := target.IsAdmin
	oldDisabled := oldPolicy != nil && oldPolicy.IsDisabled
	oldAll := oldPolicy == nil || oldPolicy.EnableAllFolders
	var oldFolders []string
	if oldPolicy != nil {
		oldFolders = oldPolicy.EnabledFolders
	}
	newAdmin := oldAdmin
	if body.IsAdministrator != nil {
		newAdmin = *body.IsAdministrator
	}
	newDisabled := oldDisabled
	if body.IsDisabled != nil {
		newDisabled = *body.IsDisabled
	}
	newAll := oldAll
	if body.EnableAllFolders != nil {
		newAll = *body.EnableAllFolders
	}
	newFolders := oldFolders
	if body.EnabledFolders != nil {
		newFolders = *body.EnabledFolders
	}

	// 不允许把最后一个可用管理员降级或禁用，避免把自己锁死。
	if oldAdmin && (!newAdmin || (newDisabled && !oldDisabled)) {
		users, _ := s.db.ListUsers(r.Context())
		admins := 0
		for _, x := range users {
			if x.IsAdmin && (x.Policy == nil || !x.Policy.IsDisabled) {
				admins++
			}
		}
		if admins <= 1 {
			writeError(w, http.StatusBadRequest, "cannot demote or disable the last administrator")
			return
		}
	}
	if err := s.db.UpdateUserPolicyPatch(r.Context(), id, body); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot update policy")
		return
	}
	// 库权限变化/禁用/降级：让该用户全部令牌失效，客户端重新登录并刷新库列表。
	if oldAll != newAll || !sameStringSet(oldFolders, newFolders) || oldDisabled != newDisabled || oldAdmin != newAdmin {
		_ = s.db.DeleteTokensForUser(r.Context(), id)
		s.dropMemTokensForUser(id)
	}
	updated, err := s.db.UserByID(r.Context(), id)
	if err != nil || updated == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	updated.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, updated)
}

// handleUpdatePassword 对应 POST /emby/Users/{UserId}/Password。
// 本人改密码需要提供 CurrentPw；管理员改他人密码可省略。
func (s *Server) handleUpdatePassword(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := r.PathValue("UserId")
	if id != u.Id && !u.IsAdmin {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	var body struct {
		CurrentPw string `json:"CurrentPw"`
		NewPw     string `json:"NewPw"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if id == u.Id && u.HasPassword {
		_, hash, err := s.db.UserByName(r.Context(), u.Name)
		if err != nil || !verifyPasswordHash(hash, body.CurrentPw) {
			writeError(w, http.StatusUnauthorized, "current password is invalid")
			return
		}
	}
	if err := s.db.SetUserPassword(r.Context(), id, body.NewPw); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot update password")
		return
	}
	// 改密后旧令牌全部失效，避免旧 token 继续有效；客户端需重新登录。
	_ = s.db.DeleteTokensForUser(r.Context(), id)
	s.dropMemTokensForUser(id)
	w.WriteHeader(http.StatusNoContent)
}

// handleRenameUser 对应 POST /emby/Users/{UserId}（管理员或本人）。
func (s *Server) handleRenameUser(w http.ResponseWriter, r *http.Request) {
	u := userFrom(r)
	if u == nil || s.db == nil {
		writeError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id := r.PathValue("UserId")
	if id != u.Id && !u.IsAdmin {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	var body struct {
		Name string `json:"Name"`
	}
	if err := decodeJSONBody(r, &body); err != nil || strings.TrimSpace(body.Name) == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	name := strings.TrimSpace(body.Name)
	if other, _, err := s.db.UserByName(r.Context(), name); err == nil && other != nil && other.Id != id {
		writeError(w, http.StatusBadRequest, "user already exists")
		return
	}
	if err := s.db.RenameUser(r.Context(), id, name); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot rename user")
		return
	}
	updated, err := s.db.UserByID(r.Context(), id)
	if err != nil || updated == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	updated.ServerId = s.serverID()
	writeJSON(w, http.StatusOK, updated)
}

// handleUserAvatar 处理 GET/POST /emby/Users/{UserId}/Images/Primary。
func (s *Server) handleUserAvatar(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("UserId")
	if s.db == nil {
		writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
		return
	}
	if r.Method == http.MethodPost {
		u := userFrom(r)
		if u == nil || (u.Id != id && !u.IsAdmin) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil || len(body) == 0 {
			writeError(w, http.StatusBadRequest, "empty image")
			return
		}
		dir := filepath.Join(s.cfg.ImageCache, "avatars")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot store avatar")
			return
		}
		p := filepath.Join(dir, id+".img")
		if err := os.WriteFile(p+".tmp", body, 0o644); err != nil {
			writeError(w, http.StatusInternalServerError, "cannot write avatar")
			return
		}
		_ = os.Rename(p+".tmp", p)
		_ = s.db.SetUserAvatar(r.Context(), id, p)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	ref, err := s.db.UserAvatar(r.Context(), id)
	if err != nil || ref == "" {
		writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
		return
	}
	body, ok := readFileLimited(ref)
	if !ok {
		writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
		return
	}
	sum := sha1.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.Header().Set("Content-Type", http.DetectContentType(body))
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(body)
}

// imageVersionTagOf 与 store 的图片版本标签保持一致（路径+大小+修改时间）。
func imageVersionTagOf(path string) string {
	fi, err := os.Stat(path)
	key := path
	if err == nil {
		key = fmt.Sprintf("%s|%d|%d", path, fi.Size(), fi.ModTime().Unix())
	}
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:8])
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, v := range a {
		m[v]++
	}
	for _, v := range b {
		m[v]--
		if m[v] < 0 {
			return false
		}
	}
	return true
}
