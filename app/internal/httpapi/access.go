package httpapi

import (
	"context"
	"net/http"
	"time"

	"emby-air/internal/model"
)

// allowedViewSet 返回用户可访问的媒体库集合与是否受限。
// nil set + restricted=false 表示不限制（管理员、未配置 policy、EnableAllFolders）。
func allowedViewSet(u *model.User) (map[string]bool, bool) {
	if u == nil || u.IsAdmin || u.Policy == nil || u.Policy.EnableAllFolders {
		return nil, false
	}
	set := make(map[string]bool, len(u.Policy.EnabledFolders))
	for _, id := range u.Policy.EnabledFolders {
		if id != "" {
			set[id] = true
		}
	}
	return set, true
}

func (s *Server) viewAllowed(ctx context.Context, u *model.User, viewID string) bool {
	set, restricted := allowedViewSet(u)
	if !restricted {
		return true
	}
	return set[viewID]
}

func (s *Server) itemAllowed(ctx context.Context, u *model.User, itemID string) bool {
	set, restricted := allowedViewSet(u)
	if !restricted {
		return true
	}
	if s.db == nil || itemID == "" {
		return false
	}
	vid, err := s.db.ItemViewID(ctx, itemID)
	if err != nil || vid == "" {
		return false
	}
	return set[vid]
}

func (s *Server) filterAllowedItems(ctx context.Context, u *model.User, items []model.Item) []model.Item {
	set, restricted := allowedViewSet(u)
	if !restricted {
		return items
	}
	out := items[:0]
	for i := range items {
		vid, err := s.db.ItemViewID(ctx, items[i].Id)
		if err == nil && set[vid] {
			out = append(out, items[i])
		}
	}
	return out
}

// allowedViewIDs 给 store 查询用的稳定切片（map 随机序不影响 IN）。
func allowedViewIDs(u *model.User) ([]string, bool) {
	set, restricted := allowedViewSet(u)
	if !restricted {
		return nil, false
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out, true
}

// requireItemAccess 在详情/播放/季集等单条入口使用；不允许时写 404，避免泄露存在性。
func (s *Server) requireItemAccess(w http.ResponseWriter, r *http.Request, itemID string) bool {
	if s.itemAllowed(r.Context(), userFrom(r), itemID) {
		return true
	}
	writeError(w, http.StatusNotFound, "item not found")
	return false
}

func (s *Server) filterAllowedViews(ctx context.Context, u *model.User, views []model.View) []model.View {
	set, restricted := allowedViewSet(u)
	if !restricted {
		return views
	}
	out := views[:0]
	for _, v := range views {
		if set[v.Id] {
			out = append(out, v)
		}
	}
	return out
}

// imageUser 解析图片请求里的令牌（header 或 api_key）。没有令牌/无效返回 nil。
func (s *Server) imageUser(r *http.Request) *model.User {
	tok := tokenFrom(r)
	if tok == "" || s.db == nil {
		return nil
	}
	if v, ok := s.memTokens.Load(tok); ok {
		if e, ok := v.(memTokenEntry); ok && time.Now().Before(e.expires) {
			return e.user
		}
		s.memTokens.Delete(tok)
	}
	u, err := s.db.UserByToken(r.Context(), tok)
	if err == nil && u != nil {
		if u.Policy != nil && u.Policy.IsDisabled {
			return nil
		}
		return u
	}
	if ok, err := s.db.ValidateAPIKey(r.Context(), tok); err == nil && ok {
		return &model.User{Id: "apikey", Name: "API", IsAdmin: true}
	}
	return nil
}

// imageAuth 保护条目/媒体库图片：带有效令牌才返回真图；缺失/无效令牌返回占位 PNG（200），
// 保持 Emby 图片契约，不会用 401 JSON 打断第三方客户端的图片加载。
func (s *Server) imageAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u := s.imageUser(r)
		if u == nil {
			writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), userCtxKey, u)))
	}
}

// imageRequestAllowed 在 handleImage 内部做条目/媒体库权限校验。
func (s *Server) imageRequestAllowed(r *http.Request, itemID string) bool {
	u := userFrom(r)
	if u == nil || s.db == nil {
		return false
	}
	if it, err := s.db.GetItem(r.Context(), itemID); err == nil && it != nil {
		return s.itemAllowed(r.Context(), u, it.Id)
	}
	return s.viewAllowed(r.Context(), u, itemID)
}
