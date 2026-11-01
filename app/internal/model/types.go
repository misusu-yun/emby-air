// Package model 定义跨包共享的数据类型。JSON 标签必须与 Emby 字段名一致。
package model

// Item 是统一条目：Movie / Series / Season / Episode / Folder / Person 都由它表示。
// ChildCount/RecursiveItemCount 是 Emby 标准字段，客户端（Filmly 等）用它们判断
// “库/条目里有没有内容”，缺失会被当成空目录直接回首页，所以不能加 omitempty（0 也要下发）。
type Item struct {
	Id                 string            `json:"Id"`
	Name               string            `json:"Name"`
	ServerId           string            `json:"ServerId,omitempty"`
	Type               string            `json:"Type"`
	Container          string            `json:"Container,omitempty"`
	CollectionType     string            `json:"CollectionType,omitempty"`
	ParentId           string            `json:"ParentId,omitempty"`
	SeriesId           string            `json:"SeriesId,omitempty"`
	SeriesName         string            `json:"SeriesName,omitempty"`
	SeasonId           string            `json:"SeasonId,omitempty"`
	Overview           string            `json:"Overview,omitempty"`
	ProductionYear     int               `json:"ProductionYear,omitempty"`
	OfficialRating     string            `json:"OfficialRating,omitempty"`
	CommunityRating    float64           `json:"CommunityRating,omitempty"`
	CriticRating       float64           `json:"CriticRating,omitempty"`
	RunTimeTicks       int64             `json:"RunTimeTicks,omitempty"`
	IndexNumber        int               `json:"IndexNumber,omitempty"`
	ParentIndexNumber  int               `json:"ParentIndexNumber,omitempty"`
	Path               string            `json:"Path,omitempty"`
	IsFolder           bool              `json:"IsFolder"`
	ChildCount         int               `json:"ChildCount"`
	RecursiveItemCount int               `json:"RecursiveItemCount"`
	Genres             []string          `json:"Genres,omitempty"`
	ImageTags          map[string]string `json:"ImageTags,omitempty"`
	BackdropImageTags  []string          `json:"BackdropImageTags,omitempty"`
	DateCreated        string            `json:"DateCreated,omitempty"`
	SortName           string            `json:"SortName,omitempty"`
	ForcedSortName     string            `json:"ForcedSortName,omitempty"`
	ProviderIds        map[string]string `json:"ProviderIds,omitempty"`
	MediaSources       []MediaSource     `json:"MediaSources,omitempty"`
	UserData           *UserData         `json:"UserData,omitempty"`

	// ---- 刮削元数据（P1：详情页/SortName/筛选）----
	PremiereDate  string   `json:"PremiereDate,omitempty"`
	OriginalTitle string   `json:"OriginalTitle,omitempty"`
	Taglines      []string `json:"Taglines,omitempty"`
	Tags          []string `json:"Tags,omitempty"`
	Studios       []NameId `json:"Studios,omitempty"`
	People        []Person `json:"People,omitempty"`

	// ---- 服务端内部字段：图像来源（不下发，omitempty 保证旧行为不变）----
	// ImageRef 指向磁盘上的封面/背景图相对 MediaRoot 的路径，由扫描器或刮削器写入；
	// 视觉上等同于 Emby 的 ImageTags，但这里直接存位置，避免每次请求 stat 目录。
	ImageRef    string `json:"-"`
	BackdropRef string `json:"-"`
	LogoRef     string `json:"-"`
}

// NameId 是 Emby 通用的 {Name, Id} 结构（Studios / Genres / Tags）。
type NameId struct {
	Name string `json:"Name"`
	Id   string `json:"Id"`
}

// Person 是演职人员条目（详情页演员表）。
type Person struct {
	Name         string `json:"Name"`
	Id           string `json:"Id"`
	Role         string `json:"Role,omitempty"`
	Type         string `json:"Type"` // Actor / Director / Writer / Producer
	PrimaryImage string `json:"-"`
}

// MediaSource 描述一个可播放来源。STRM 场景下 Path 即远端直链。
type MediaSource struct {
	Id   string `json:"Id"`
	Name string `json:"Name"`
	Path string `json:"Path"`
	// LocalPath 是扫描时的本地 .strm/媒体文件路径，不对外输出；
	// Path 在响应前可能被换成远端直链，外挂字幕必须靠它定位同目录文件。
	LocalPath string `json:"-"`
	Container string `json:"Container,omitempty"`
	// Protocol / IsRemote 是 Emby 标准字段：客户端用它们判断“要不要走服务端代理”。
	// 网盘 STRM 直链 → Http/true；本地文件 → File/false。
	Protocol                   string        `json:"Protocol,omitempty"`
	IsRemote                   bool          `json:"IsRemote,omitempty"`
	Size                       int64         `json:"Size,omitempty"`
	RunTimeTicks               int64         `json:"RunTimeTicks,omitempty"`
	Bitrate                    int64         `json:"Bitrate,omitempty"`
	SupportsDirectPlay         bool          `json:"SupportsDirectPlay"`
	DirectStreamUrl            string        `json:"DirectStreamUrl,omitempty"`
	SupportsDirectStream       bool          `json:"SupportsDirectStream"`
	SupportsTranscoding        bool          `json:"SupportsTranscoding"`
	MediaStreams               []MediaStream `json:"MediaStreams,omitempty"`
	DefaultAudioStreamIndex    *int          `json:"DefaultAudioStreamIndex,omitempty"`
	DefaultSubtitleStreamIndex *int          `json:"DefaultSubtitleStreamIndex,omitempty"`
}

// MediaStream 是容器内的一条流（视频/音频/字幕）。
type MediaStream struct {
	Index      int    `json:"Index"`
	Type       string `json:"Type"` // Video / Audio / Subtitle
	Codec      string `json:"Codec,omitempty"`
	Language   string `json:"Language,omitempty"`
	Title      string `json:"Title,omitempty"`
	IsDefault  bool   `json:"IsDefault"`
	IsForced   bool   `json:"IsForced"`
	Width      int    `json:"Width,omitempty"`
	Height     int    `json:"Height,omitempty"`
	Channels   int    `json:"Channels,omitempty"`
	BitDepth   int    `json:"BitDepth,omitempty"`
	VideoRange string `json:"VideoRange,omitempty"` // SDR / HDR
	// VideoRangeType 是 Emby/Jellyfin 的枚举：SDR / HDR10 / HLG / DOVI。
	VideoRangeType string `json:"VideoRangeType,omitempty"`
	ColorSpace     string `json:"ColorSpace,omitempty"`
	ColorTransfer  string `json:"ColorTransfer,omitempty"`
	ColorPrimaries string `json:"ColorPrimaries,omitempty"`
	IsExternal     bool   `json:"IsExternal"`
	// DeliveryMethod / DeliveryUrl 是 Emby 外挂/可下载字幕的标准字段。
	// 外挂字幕填 External + 服务端下载地址；内嵌字幕后续可复用同一套接口。
	DeliveryMethod   string  `json:"DeliveryMethod,omitempty"`
	DeliveryUrl      string  `json:"DeliveryUrl,omitempty"`
	PixelFormat      string  `json:"PixelFormat,omitempty"`
	AspectRatio      string  `json:"AspectRatio,omitempty"`
	AverageFrameRate float32 `json:"AverageFrameRate,omitempty"`

	// DisplayTitle 是 Emby/Jellyfin 的标准流标题；部分客户端（Filmly）按它展示版本/音轨，
	// 不单看 MediaSource.Name。仅 Filmly 分支会填（其它客户端保持不变）。
	DisplayTitle string `json:"DisplayTitle,omitempty"`
}

// UserData 是「当前用户视角」的条目状态。
type UserData struct {
	// 与 Emby/Jellyfin 的 UserItemDataDto 对齐：客户端可能依赖 ItemId / PlayedPercentage
	// 来判定“可续播”，缺字段会导致详情页显示“播放”而不是“继续播放”。
	ItemId                string  `json:"ItemId"`
	PlaybackPositionTicks int64   `json:"PlaybackPositionTicks"`
	PlayedPercentage      float64 `json:"PlayedPercentage"`
	PlayCount             int     `json:"PlayCount"`
	IsFavorite            bool    `json:"IsFavorite"`
	Played                bool    `json:"Played"`
	LastPlayedDate        string  `json:"LastPlayedDate,omitempty"`
	Key                   string  `json:"Key,omitempty"`
}

// UserPolicy 对应 Emby 的 UserPolicy：账号能做什么、能看哪些库。
type UserPolicy struct {
	IsAdministrator   bool     `json:"IsAdministrator"`
	IsDisabled        bool     `json:"IsDisabled"`
	MaxActiveSessions int      `json:"MaxActiveSessions"`
	EnableAllFolders  bool     `json:"EnableAllFolders"`
	EnabledFolders    []string `json:"EnabledFolders,omitempty"`
	BlockedTags       []string `json:"BlockedTags,omitempty"`
}

// User 是账号。密码只以哈希形式存在。
type User struct {
	Id              string      `json:"Id"`
	Name            string      `json:"Name"`
	ServerId        string      `json:"ServerId,omitempty"`
	HasPassword     bool        `json:"HasPassword"`
	IsAdmin         bool        `json:"IsAdmin"`
	Policy          *UserPolicy `json:"Policy,omitempty"`
	PrimaryImageTag string      `json:"PrimaryImageTag,omitempty"`
}

// View 是媒体库（Emby 的 UserView）。
type View struct {
	Id             string   `json:"Id"`
	Name           string   `json:"Name"`
	CollectionType string   `json:"CollectionType,omitempty"`
	Paths          []string `json:"Paths,omitempty"`
	Type           string   `json:"Type"`
	IsFolder       bool     `json:"IsFolder"`
	SortOrder      int      `json:"-"`
}

// ItemsQuery 是列表查询参数（已从 HTTP 层解析、规范化后的形态）。
type ItemsQuery struct {
	UserId   string
	ParentId string
	Types    []string // IncludeItemTypes
	// AllowedParents 非空时把结果限定在这些媒体库 ID 下（用于普通用户的库权限）。
	// ParentId 为空（全局/搜索）时生效；带 ParentId 时由 HTTP 层先校验父级归属。
	AllowedParents []string
	// ExcludeTypes 对应 Emby 的 ExcludeItemTypes：命中这些 kind 的条目不返回。
	ExcludeTypes []string
	Recursive    bool
	SortBy       []string // 已展开的多字段排序，如 ["DateCreated","SortName"]
	SortOrder    string   // Ascending | Descending
	StartIndex   int
	Limit        int
	SearchTerm   string
	Filters      []string
	Genres       []string
	Years        []int
	IsPlayed     *bool
	IsFavorite   *bool
	Fields       []string
	// Ids 非空时只返回这些条目（Emby 的 ?Ids=a,b,c，客户端常用它批量取最新状态）。
	Ids []string
	// AnyProviderIdEquals：形如 tmdb.12345，用于按外部 ID 精确查条目。
	AnyProviderId string
	// MinDateLastSaved：只返回该时间之后更新过的条目（Pansave 增量轮询用）。
	MinDateLastSaved int64
}

// Page 是列表结果。
type Page struct {
	Items            []Item `json:"Items"`
	TotalRecordCount int    `json:"TotalRecordCount"`
	StartIndex       int    `json:"StartIndex"`
}

// Session 是一条播放会话。
type Session struct {
	Id             string `json:"Id"`
	UserId         string `json:"UserId,omitempty"`
	DeviceName     string `json:"DeviceName,omitempty"`
	Client         string `json:"Client,omitempty"`
	NowPlayingItem *Item  `json:"NowPlayingItem,omitempty"`
	PlayState      struct {
		PositionTicks int64 `json:"PositionTicks"`
		IsPaused      bool  `json:"IsPaused"`
	} `json:"PlayState"`
}
