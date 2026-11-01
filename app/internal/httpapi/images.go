package httpapi

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"emby-air/internal/model"
)

// imageAssets 保存内置占位图字节，避免每个请求重新生成。
type imageAssets struct {
	// transparentPNG 是 1×1 透明 PNG。图片缺失时必须返回它，绝不能返回 HTML。
	transparentPNG []byte
}

// buildImageAssets 构造内置占位图（启动时一次）。
func buildImageAssets() imageAssets {
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{0, 0, 0, 0})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		// png.Encode 对 1×1 RGBA 不会失败；若失败仍给出硬编码的合法 PNG。
		return imageAssets{transparentPNG: hardcodedTransparentPNG}
	}
	return imageAssets{transparentPNG: buf.Bytes()}
}

// hardcodedTransparentPNG 是 1×1 透明 PNG 的标准字节序列（兜底）。
var hardcodedTransparentPNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
	0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
	0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
	0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00,
	0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
	0x42, 0x60, 0x82,
}

// ImageType 名称到候选文件名（按优先级）的映射。
var imageCandidates = map[string][]string{
	"primary":    {"poster.jpg", "folder.jpg", "poster.png", "folder.png"},
	"backdrop":   {"backdrop.jpg", "backdrop.png", "fanart.jpg", "fanart.png"},
	"thumb":      {"thumb.jpg", "landscape.jpg", "backdrop.jpg"},
	"logo":       {"logo.png", "clearlogo.png", "logo.jpg"},
	"banner":     {"banner.jpg", "banner.png"},
	"art":        {"clearlogo.png", "logo.png", "clearart.png"},
	"disc":       {"disc.png", "discart.png"},
	"boxart":     {"boxart.jpg"},
	"screenshot": {"screenshot.jpg"},
	"menu":       {"menu.jpg"},
	"chapter":    {"chapter.jpg"},
}

func (s *Server) acquireImageSlot(r *http.Request) bool {
	if s.imageSem == nil {
		return true
	}
	select {
	case s.imageSem <- struct{}{}:
		return true
	case <-r.Context().Done():
		return false
	}
}

func (s *Server) releaseImageSlot() {
	if s.imageSem != nil {
		<-s.imageSem
	}
}

// handleImage 处理 GET/HEAD /emby/Items/{Id}/Images/{Type} 以及路径式变体
// /emby/Items/{Id}/Images/{Type}/{Index}/{Tag}/{Format}/{MaxWidth}/{MaxHeight}/{...}。
// 缺失一律返回 1×1 透明 PNG（200），绝不返回 HTML。
func (s *Server) handleImage(w http.ResponseWriter, r *http.Request) {
	// 图片端点是“尽力而为”的资源：鉴权失败也返回占位图会泄露条目存在性，
	// 但 Emby 客户端常在不带 token 时拉图；这里要求令牌或占位图二选一 ——
	// 契约规定 404 必须返回 PNG，因此无令牌时同样返回占位图。
	id := r.PathValue("Id")
	imgType := strings.ToLower(strings.TrimSpace(r.PathValue("Type")))
	rest := r.PathValue("rest")
	if imgType == "" {
		// /emby/Items/{Id}/Images 这条路径没有 Type，取 Primary。
		imgType = "primary"
	}
	if !s.acquireImageSlot(r) {
		return
	}
	defer s.releaseImageSlot()

	// 路径式参数：Type/Index/Tag/Format/MaxWidth/MaxHeight/PercentPlayed/UnplayedCount
	// 只取我们需要的前几段；其余忽略（客户端可能塞任意尾巴）。
	var maxWidth int
	if rest != "" {
		segs := strings.Split(rest, "/")
		// segs[0]=Index, segs[1]=Tag, segs[2]=Format, segs[3]=MaxWidth, segs[4]=MaxHeight
		// segs[1] 是 Tag，只用于缓存键校验；这里不需要。
		if len(segs) > 3 {
			maxWidth = atoiDefault(segs[3], 0)
		}
	}
	if v := getCI(r, "maxWidth"); v != "" {
		maxWidth = atoiDefault(v, maxWidth)
	}
	// 普通用户只能取授权媒体库/条目的图片；未授权或条目不存在返回占位图。
	if !s.imageRequestAllowed(r, id) {
		writePlaceholderPNG(w, r, s.images.transparentPNG, r.Method == http.MethodHead)
		return
	}
	// 媒体库封面：库不是 item，文件放在 cache/libraries/<id>.img。
	// 必须用 no-cache 提供，否则上传新封面后浏览器/客户端会一直用旧缓存。
	if imgType == "primary" || imgType == "poster" || imgType == "" {
		if cover, ok := readFileLimited(filepath.Join(s.cfg.ImageCache, "libraries", id+".img")); ok {
			sum := sha1.Sum(cover)
			tag := `"` + hex.EncodeToString(sum[:]) + `"`
			w.Header().Set("ETag", tag)
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Content-Type", http.DetectContentType(cover))
			if im := r.Header.Get("If-None-Match"); im != "" && ifNoneMatch(im, tag) {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Length", strconv.Itoa(len(cover)))
				w.WriteHeader(http.StatusOK)
				return
			}
			_, _ = w.Write(cover)
			return
		}
	}

	body, etag, ok := s.findImage(r, id, imgType, maxWidth)
	// Moonfin 保持既有行为：maxWidth<=512 时缩到请求宽度。
	thumbWidth := 0
	if ok && isMoonfin(r) && maxWidth > 0 && maxWidth <= 512 && thumbableImageType(imgType) {
		thumbWidth = maxWidth
	}
	// 爆米花/Filmly 拉图常常不带 maxWidth：海报缩到 480 省流量，但详情页背景图
	// 同样被缩成 480 就极其模糊。这里按图片类型分流（0 = 不缩放，返回原图）。
	if ok && s.isPopcornClient(r) {
		thumbWidth = popcornThumbWidth(imgType, maxWidth)
	}
	// VidHub 列表页会一次预取大量海报；它通常不带 maxWidth，原图 200~500KB/张
	// 会把带宽排队，视觉上就是“上面的海报迟迟不出来”。给 VidHub 发 720px 缩略图。
	if ok && isVidHub(r) {
		thumbWidth = vidhubThumbWidth(imgType, maxWidth)
	}
	if thumbWidth > 0 {
		if thumb, changed := s.clientThumb(body, id, imgType, thumbWidth); changed {
			body = thumb
			etag = ""
		}
	}
	if ok && etag == "" {
		sum := sha1.Sum(body)
		etag = `"` + hex.EncodeToString(sum[:]) + `"`
	}
	if !ok {
		// 缺失：透明 PNG + 长缓存（客户端不会反复打服务器）。
		// 日志按 (itemID, imgType) 有界去重：缺图多时不会把请求卡在日志锁上。
		if missingImageLog.shouldLog(id, imgType) {
			log.Printf("图片缺失：item=%s 类型=%s 宽度=%d（返回占位图）", id, imgType, maxWidth)
		}
		writePlaceholderPNG(w, r, s.images.transparentPNG, false)
		return
	}

	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	if im := r.Header.Get("If-None-Match"); im != "" && etag != "" && ifNoneMatch(im, etag) {
		w.Header().Set("Cache-Control", imageCacheControl)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	if isJPEG(body) {
		w.Header().Set("Content-Type", "image/jpeg")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Cache-Control", imageCacheControl)
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	_, _ = w.Write(body)
}

const imageCacheControl = "public, max-age=2592000, immutable" // 30 天

const (
	// missingImageLogWindow 是同一个 (itemID, imgType) 缺图日志的去重窗口。
	missingImageLogWindow = 10 * time.Minute
	// missingImageLogMaxKeys 是去重集合的硬上限（约 512 × 几十字节，可忽略）。
	missingImageLogMaxKeys = 512
)

// missingImageLogState 按 itemID|imgType 去掉重复的“图片缺失”日志。
// 缺图往往成百上千（一次首页加载 = 几十个条目 × 多个图片类型），每张都 log.Printf
// 会与全局 logger 互斥、并同步写 stderr，请求就被日志拖死了。
// 这里 10 分钟内每个 key 只记一次，最多 512 个 key（过期的先清理，仍满则放弃本次日志）。
type missingImageLogState struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// missingImageLog 是进程级去重状态（Server 可能被多个测试实例重复创建，去重表与实例无关）。
var missingImageLog = &missingImageLogState{}

// shouldLog 返回本次缺图是否应当写日志（同窗口内同一个 key 只返回一次 true）。
func (m *missingImageLogState) shouldLog(itemID, imgType string) bool {
	key := itemID + "|" + imgType
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = make(map[string]time.Time, 64)
	}
	if t, ok := m.seen[key]; ok && now.Sub(t) < missingImageLogWindow {
		return false
	}
	if len(m.seen) >= missingImageLogMaxKeys {
		for k, t := range m.seen {
			if now.Sub(t) >= missingImageLogWindow {
				delete(m.seen, k)
			}
		}
		if len(m.seen) >= missingImageLogMaxKeys {
			// 窗口内的 key 太多：本次不记日志，绝不让 map 无界增长。
			return false
		}
	}
	m.seen[key] = now
	return true
}

const (
	// popcornPosterThumbWidth 是海报/缩略图不带 maxWidth 时的默认宽度
	// （手机/平板上够用，体积约为原图的 1/10）。
	popcornPosterThumbWidth = 480
	// popcornPosterMaxThumbWidth 是海报/缩略图宽度上限（防止异常大的 maxWidth 反而更耗时）。
	popcornPosterMaxThumbWidth = 1024
	// popcornBackdropMaxThumbWidth 是背景图宽度上限：详情页背景是大图，
	// 最高只允许缩到 4K 宽，再大就按原图给。
	popcornBackdropMaxThumbWidth = 3840
)

// thumbableImageType 判断某类图片是否适合做缩略图。Logo/Art 等是带 alpha 的
// PNG，缩略图路径会转成 JPEG 并把透明区域压成黑底，所以必须返回原图。
func thumbableImageType(imgType string) bool {
	switch strings.ToLower(strings.TrimSpace(imgType)) {
	case "", "primary", "poster", "backdrop", "background", "fanart", "thumb":
		return true
	}
	return false
}

// popcornThumbWidth 计算爆米花缩略图目标宽度；返回 0 表示不缩放（直接返回原图）。
// 线上背景图（Backdrop）被一律缩到 480px，详情页非常模糊，所以按类型分流：
//   - primary/poster（空类型同样按 Primary）：默认 480，显式 maxWidth 上限 1024；
//   - backdrop/background/fanart：默认不缩放，显式 maxWidth 时上限 3840；
//   - thumb：与海报一致（默认 480，上限 1024）；
//   - logo/art/disc/banner/boxart/screenshot/menu/chapter 等：一律不缩放
//     （本来是小图，或带透明通道，缩放没收益且会糊）。
func popcornThumbWidth(imgType string, maxWidth int) int {
	switch imgType {
	case "", "primary", "poster", "thumb":
		return popcornClampThumbWidth(maxWidth, popcornPosterThumbWidth, popcornPosterMaxThumbWidth)
	case "backdrop", "background", "fanart":
		if maxWidth <= 0 {
			return 0
		}
		return popcornClampThumbWidth(maxWidth, 0, popcornBackdropMaxThumbWidth)
	default:
		return 0
	}
}

// popcornClampThumbWidth 在 [def, limit] 间取目标宽度：
// 未指定 maxWidth 时用默认值，指定时按请求但不超过上限。
func popcornClampThumbWidth(maxWidth, def, limit int) int {
	if maxWidth <= 0 {
		return def
	}
	if maxWidth > limit {
		return limit
	}
	return maxWidth
}

// vidhubThumbWidth 给 VidHub 列表海报一个默认缩略宽度。
// VidHub 不满屏显示时用 720px 足够；请求更大时最多 1024px，避免 4K 原图占满隧道。
// 背景图仍保留清晰度：不带 maxWidth 时不缩放。
func vidhubThumbWidth(imgType string, maxWidth int) int {
	switch strings.ToLower(strings.TrimSpace(imgType)) {
	case "", "primary", "poster", "thumb":
		if maxWidth <= 0 {
			return 720
		}
		if maxWidth > 1024 {
			return 1024
		}
		return maxWidth
	case "backdrop", "background", "fanart":
		if maxWidth <= 0 {
			return 0
		}
		if maxWidth > 1920 {
			return 1920
		}
		return maxWidth
	}
	return 0
}

func writePlaceholderPNG(w http.ResponseWriter, r *http.Request, png []byte, isHead bool) {
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.Itoa(len(png)))
	// 占位图只缓存 60 秒：刮削完成后客户端能很快拿到真封面。
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("ETag", placeholderETag)
	if im := r.Header.Get("If-None-Match"); im != "" && ifNoneMatch(im, placeholderETag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if isHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write(png)
}

const placeholderETag = `"placeholder-1x1"`

func ifNoneMatch(header, etag string) bool {
	for _, part := range strings.Split(header, ",") {
		p := strings.TrimSpace(part)
		p = strings.TrimPrefix(p, "W/")
		if p == etag || p == "*" {
			return true
		}
	}
	return false
}

func isJPEG(b []byte) bool { return len(b) > 3 && b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF }

// findImage 在 MediaRoot 下按条目路径找到真实图片。
// maxWidth>0 时优先同一目录里最接近宽度的 "poster<W>.jpg" 变体（不缩放）。
func (s *Server) findImage(r *http.Request, itemID, imgType string, maxWidth int) ([]byte, string, bool) {
	if s.db == nil || s.cfg.MediaRoot == "" {
		return nil, "", false
	}
	info, err := s.db.ItemImageInfo(r.Context(), itemID)
	if err != nil || info == nil {
		// 媒体库封面：库本身不是 item，单独存在 cache/libraries/<id>.img。
		if body, ok := readFileLimited(filepath.Join(s.cfg.ImageCache, "libraries", itemID+".img")); ok {
			sum := sha1.Sum(body)
			return body, `"` + hex.EncodeToString(sum[:]) + `"`, true
		}
		return nil, "", false
	}
	// 图片端点只需要路径与图片引用，不能为 100 张封面重复读取整行及 media_sources。
	it := &model.Item{Id: itemID, Type: info.Type, Path: info.Path,
		ImageRef: info.ImageRef, BackdropRef: info.BackdropRef, LogoRef: info.LogoRef}
	// 0) 单集缩略图按需下载：TMDB still_path 存在但本地没有缓存时，拉一次 w300 并落盘。
	// 只在客户端真的请求这张图时发生，避免刮削时批量下载 6 万张图（磁盘/带宽/内存都扛不住）。
	if imgType == "primary" || imgType == "thumb" {
		if body, ok := s.ensureStillImage(r.Context(), it.Id); ok {
			sum := sha1.Sum(body)
			return body, `"` + hex.EncodeToString(sum[:]) + `"`, true
		}
	}
	// 按请求类型找图：1) 条目记录的图片位置 2) 同目录候选文件名 3) *_<type>.jpg 兜底。
	dir := s.mediaDir(it.Path)
	if dir == "" {
		return nil, "", false
	}
	if body, etag, ok := s.findImageInDir(it, dir, imgType); ok {
		return body, etag, true
	}
	// 4) primary ↔ backdrop 回退：很多条目只刮到一张图（线上 8470 部电影里 4304 部没有海报），
	// 详情页请求 Backdrop 时不能因此退回占位图。thumb/logo 等其它类型不参与回退。
	if alt := imageFallbackType(imgType); alt != "" {
		return s.findImageInDir(it, dir, alt)
	}
	return nil, "", false
}

// findImageInDir 在条目的目录/图片引用里找指定类型的图片。
func (s *Server) findImageInDir(it *model.Item, dir, imgType string) ([]byte, string, bool) {
	// 1) 优先用条目上记录的图片位置（刮削器/扫描器写入）：一次 stat，无目录扫描。
	if ref := pickImageRef(it, imgType); ref != "" {
		if p, ok := s.resolveImagePath(ref); ok {
			if body, ok := readFileLimited(p); ok {
				sum := sha1.Sum(body)
				return body, `"` + hex.EncodeToString(sum[:]) + `"`, true
			}
		}
	}
	cands := imageCandidates[imgType]
	if len(cands) == 0 {
		cands = imageCandidates["primary"]
	}
	for _, name := range cands {
		if body, ok := readFileLimited(filepath.Join(dir, name)); ok {
			sum := sha1.Sum(body)
			return body, `"` + hex.EncodeToString(sum[:]) + `"`, true
		}
	}
	// 分季/分集图：<dir>/<imgType>N.jpg 之类由候选名已覆盖；再退一步找 *_<type>.jpg。
	if entries, err := os.ReadDir(dir); err == nil {
		suffix := "_" + imgType + ".jpg"
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), suffix) {
				continue
			}
			if body, ok := readFileLimited(filepath.Join(dir, e.Name())); ok {
				sum := sha1.Sum(body)
				return body, `"` + hex.EncodeToString(sum[:]) + `"`, true
			}
		}
	}
	return nil, "", false
}

// imageFallbackType 返回图片类型的回退类型：有海报没背景图时用海报当背景，反之亦然。
func imageFallbackType(imgType string) string {
	switch imgType {
	case "primary", "poster", "":
		return "backdrop"
	case "backdrop", "background", "fanart":
		return "primary"
	}
	return ""
}

const maxImageBytes = 32 << 20 // 32 MiB 上限，防止异常文件打爆内存

func readFileLimited(p string) ([]byte, bool) {
	fi, err := os.Stat(p)
	if err != nil || fi.IsDir() || fi.Size() <= 0 || fi.Size() > maxImageBytes {
		return nil, false
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, false
	}
	return b, true
}

// pickImageRef 根据图片类型选择条目记录的图片位置。
func pickImageRef(it *model.Item, imgType string) string {
	switch imgType {
	case "logo", "art", "clearlogo":
		if it.LogoRef != "" {
			return it.LogoRef
		}
		return it.ImageRef
	case "backdrop", "background", "fanart":
		return it.BackdropRef
	case "thumb":
		if it.BackdropRef != "" {
			return it.BackdropRef
		}
		return it.ImageRef
	case "primary", "poster", "":
		return it.ImageRef
	}
	return ""
}

// resolveImagePath 把图片引用解析成绝对路径。两种形式：
//   - 绝对路径（扫描器写入的本地 poster.jpg / fanart.jpg）
//   - 相对 ImageCache 的路径（刮削器写入的 <server>/<itemid>_poster.jpg）
//
// 两种都在各自的根下校验，越界一律拒绝。
func (s *Server) resolveImagePath(ref string) (string, bool) {
	if ref == "" {
		return "", false
	}
	if filepath.IsAbs(ref) {
		if s.cfg.MediaRoot == "" {
			return "", false
		}
		if !withinRoot(filepath.Clean(s.cfg.MediaRoot), filepath.Clean(ref)) {
			return "", false
		}
		return ref, true
	}
	if s.cfg.ImageCache == "" {
		return "", false
	}
	p := filepath.Join(s.cfg.ImageCache, ref)
	if !withinRoot(filepath.Clean(s.cfg.ImageCache), filepath.Clean(p)) {
		return "", false
	}
	return p, true
}

// mediaDir 由条目路径推出媒体目录：
//
//	STRM 条目 Path 指向 .strm 文件 → 取其所在目录；
//	目录条目（Folder/Series/Season）→ 其路径本身。
//
// 路径必须在 MediaRoot 之内；否则视为不可用（不做任何越权读取）。
func (s *Server) mediaDir(itemPath string) string {
	p := filepath.Clean(itemPath)
	root := filepath.Clean(s.cfg.MediaRoot)
	if !filepath.IsAbs(p) {
		p = filepath.Join(root, p)
	}
	if !withinRoot(root, p) {
		return ""
	}
	if fi, err := os.Stat(p); err == nil && fi.IsDir() {
		return p
	}
	return filepath.Dir(p)
}

// withinRoot 判断 p 是否在 root 内（防目录穿越）。
func withinRoot(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// handleHeadImage 处理 HEAD 请求：与 GET 相同，但不能写 body。
// 先取长度，再只写状态；缺失时同样返回占位图的大小。
func (s *Server) handleHeadImage(w http.ResponseWriter, r *http.Request) {
	r = r.Clone(r.Context())
	r.Method = http.MethodHead

	id := r.PathValue("Id")
	imgType := strings.ToLower(strings.TrimSpace(r.PathValue("Type")))
	rest := r.PathValue("rest")
	if imgType == "" {
		imgType = "primary"
	}
	if !s.acquireImageSlot(r) {
		return
	}
	defer s.releaseImageSlot()
	maxWidth := 0
	if rest != "" {
		segs := strings.Split(rest, "/")
		if len(segs) > 3 {
			maxWidth = atoiDefault(segs[3], 0)
		}
	}
	if v := getCI(r, "maxWidth"); v != "" {
		maxWidth = atoiDefault(v, maxWidth)
	}

	body, etag, ok := s.findImage(r, id, imgType, maxWidth)
	// HEAD 必须与 GET 的 body 长度一致，否则客户端预检会误判。
	thumbWidth := 0
	if ok && isMoonfin(r) && maxWidth > 0 && maxWidth <= 512 && thumbableImageType(imgType) {
		thumbWidth = maxWidth
	}
	if ok && s.isPopcornClient(r) {
		thumbWidth = popcornThumbWidth(imgType, maxWidth)
	}
	if ok && isVidHub(r) {
		thumbWidth = vidhubThumbWidth(imgType, maxWidth)
	}
	if thumbWidth > 0 {
		if thumb, changed := s.clientThumb(body, id, imgType, thumbWidth); changed {
			body, etag = thumb, ""
		}
	}
	if !ok {
		writePlaceholderPNG(w, r, s.images.transparentPNG, true)
		return
	}
	ct := "image/png"
	if isJPEG(body) {
		ct = "image/jpeg"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Cache-Control", imageCacheControl)
	if etag == "" {
		// 缩略图替换了 body：按新 body 重算 ETag（与 GET 保持一致）。
		sum := sha1.Sum(body)
		etag = `"` + hex.EncodeToString(sum[:]) + `"`
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
		if im := r.Header.Get("If-None-Match"); im != "" && ifNoneMatch(im, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// ensureStillImage 按需下载并缓存单集缩略图。
// 并发控制：同一 item 同时只下载一次（singleflight）；文件落盘后后续请求直接读。
func (s *Server) ensureStillImage(ctx context.Context, itemID string) ([]byte, bool) {
	if s.cfg.StillFetcher == nil {
		return nil, false
	}
	dir := filepath.Join(s.cfg.ImageCache, s.serverID())
	cached := filepath.Join(dir, itemID+"_still.jpg")
	if body, ok := readFileLimited(cached); ok {
		return body, true
	}
	path := s.db.StillPathOf(ctx, itemID)
	if path == "" {
		return nil, false
	}
	// 每个 item 一把锁，避免同一张图被并发下载多次。
	v, _ := s.stillLocks.LoadOrStore(itemID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer func() { mu.Unlock(); s.stillLocks.Delete(itemID) }()

	if body, ok := readFileLimited(cached); ok { // 等锁期间可能已被别的请求写好
		return body, true
	}
	body, err := s.cfg.StillFetcher(ctx, path)
	if err != nil || len(body) == 0 || len(body) > 4<<20 {
		return nil, false
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false
	}
	tmp := cached + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return nil, false
	}
	if err := os.Rename(tmp, cached); err != nil {
		return nil, false
	}
	return body, true
}

// clientThumb 按 maxWidth 生成并缓存缩略图（Moonfin / 爆米花请求共用）。
// 原始海报常有 350KB~1.1MB，首页几十张图在隧道上就是数 MB；缩略图约 10~20KB。
func (s *Server) clientThumb(body []byte, id, imgType string, maxWidth int) ([]byte, bool) {
	if s.cfg.ImageCache == "" || len(body) < 8*1024 {
		return body, false
	}
	dir := filepath.Join(s.cfg.ImageCache, "thumbs")
	_ = os.MkdirAll(dir, 0o755)
	sum := sha1.Sum(body)
	cachePath := filepath.Join(dir, fmt.Sprintf("%s_%s_%d_%x.jpg", id, imgType, maxWidth, sum[:8]))
	if cached, err := os.ReadFile(cachePath); err == nil && len(cached) > 0 {
		// 命中时刷新 mtime：图片缓存清理按最后访问时间淘汰，避免常用缩略图被误删。
		now := time.Now()
		_ = os.Chtimes(cachePath, now, now)
		return cached, true
	}
	src, _, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		return body, false
	}
	b := src.Bounds()
	if b.Dx() <= maxWidth {
		return body, false
	}
	w := maxWidth
	h := b.Dy() * maxWidth / b.Dx()
	if h <= 0 {
		return body, false
	}
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	// 最近邻采样：缩略图尺寸小，速度优先，避免引入额外的图像库依赖。
	for y := 0; y < h; y++ {
		sy := b.Min.Y + y*b.Dy()/h
		for x := 0; x < w; x++ {
			sx := b.Min.X + x*b.Dx()/w
			dst.Set(x, y, src.At(sx, sy))
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: 82}); err != nil {
		return body, false
	}
	out := buf.Bytes()
	_ = os.WriteFile(cachePath, out, 0o644)
	return out, true
}
