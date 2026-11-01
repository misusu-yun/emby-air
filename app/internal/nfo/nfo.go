// Package nfo 解析 Kodi/Emby 兼容的 .nfo 文件。
//
// 支持的根元素：<movie> <tvshow> <episod> <episode>。缺失字段一律零值，
// 调用方以“有值才覆盖”的语义合并，因此本包不区分“未设置”和“设置成空”。
//
// 视图字段（<streamdetails> / <fileinfo>）只在存在时填充，用于把码率、分辨率、
// 音轨、字幕轨喂给 MediaSources，避免客户端把 STRM 当成未知封装。
package nfo

import (
	"encoding/xml"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

// Data 是一次 NFO 解析的结果。
type Data struct {
	Title         string
	OriginalTitle string
	SortTitle     string
	Outline       string
	Plot          string
	Tagline       string
	Year          int
	Premiered     string // YYYY-MM-DD
	Rating        float64
	CriticRating  float64
	RuntimeMin    int
	MPAA          string
	Genres        []string
	Tags          []string
	Studios       []string
	Directors     []string
	Writers       []string
	Actors        []Actor
	UniqueIDs     map[string]string // tmdb / imdb / tvdb
	Season        int
	Episode       int
	ShowTitle     string
	Streams       []Stream
}

// Actor 是 <actor>。
type Actor struct {
	Name      string
	Role      string
	SortOrder int
}

// Stream 是 <streamdetails> 下的一条流。
type Stream struct {
	Type     string // video / audio / subtitle
	Codec    string
	Width    int
	Height   int
	Channels int
	Language string
	Title    string
	Default  bool
	Forced   bool
}

// ErrNotFound 表示 NFO 文件不存在。
var ErrNotFound = errors.New("nfo: not found")

// Load 读取并解析 NFO；文件不存在返回 ErrNotFound，解析失败返回错误。
func Load(path string) (*Data, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// NFO 超过 4 MB 的极罕见；截断以防异常文件。
	return Parse(io.LimitReader(f, 4<<20), fi.Size())
}

// Parse 解析 NFO 内容。size 仅用于拒绝空文件。
func Parse(r io.Reader, size int64) (*Data, error) {
	if size == 0 {
		return nil, errors.New("nfo: empty file")
	}
	var raw root
	if err := xml.NewDecoder(r).Decode(&raw); err != nil {
		return nil, err
	}
	d := &Data{
		Title:         text(raw.Title),
		OriginalTitle: text(raw.OriginalTitle),
		SortTitle:     text(raw.SortTitle),
		Outline:       text(raw.Outline),
		Plot:          text(raw.Plot),
		Tagline:       text(raw.Tagline),
		Premiered:     normalizeDate(text(raw.Premiered)),
		MPAA:          text(raw.MPAA),
	}
	d.Year = atoi(text(raw.Year))
	if d.Year == 0 && len(d.Premiered) >= 4 {
		d.Year, _ = strconv.Atoi(d.Premiered[:4])
	}
	d.Rating = atof(text(raw.Rating))
	d.CriticRating = atof(text(raw.CriticRating))
	d.RuntimeMin = atoi(text(raw.Runtime))
	d.Season = atoi(text(raw.Season))
	d.Episode = atoi(text(raw.Episode))
	d.ShowTitle = text(raw.ShowTitle)

	for _, g := range raw.Genres() {
		if g = strings.TrimSpace(g); g != "" {
			d.Genres = append(d.Genres, g)
		}
	}
	for _, t := range raw.Tags() {
		if t = strings.TrimSpace(t); t != "" {
			d.Tags = append(d.Tags, t)
		}
	}
	for _, s := range raw.Studios() {
		if s = strings.TrimSpace(s); s != "" {
			d.Studios = append(d.Studios, s)
		}
	}
	for _, c := range raw.Credits() {
		if c = strings.TrimSpace(c); c != "" {
			d.Writers = append(d.Writers, c)
		}
	}
	for _, dir := range raw.Directors() {
		if dir = strings.TrimSpace(dir); dir != "" {
			d.Directors = append(d.Directors, dir)
		}
	}
	for _, a := range raw.Actor {
		name := strings.TrimSpace(text(a.Name))
		if name == "" {
			continue
		}
		d.Actors = append(d.Actors, Actor{Name: name, Role: strings.TrimSpace(text(a.Role)), SortOrder: atoi(text(a.SortOrder))})
	}
	for _, u := range raw.UniqueID {
		k := strings.ToLower(strings.TrimSpace(u.Type))
		v := strings.TrimSpace(text(u.Value))
		if k == "" || v == "" {
			continue
		}
		if d.UniqueIDs == nil {
			d.UniqueIDs = map[string]string{}
		}
		d.UniqueIDs[k] = v
	}
	for _, s := range raw.FileInfo.StreamDetails.Video {
		d.Streams = append(d.Streams, Stream{
			Type: "Video", Codec: strings.ToLower(text(s.Codec)),
			Width: atoi(text(s.Width)), Height: atoi(text(s.Height)),
			Default: true,
		})
	}
	for i, s := range raw.FileInfo.StreamDetails.Audio {
		d.Streams = append(d.Streams, Stream{
			Type: "Audio", Codec: strings.ToLower(text(s.Codec)),
			Channels: atoi(text(s.Channels)), Language: defaultLang(text(s.Language)),
			Title: text(s.Title), Default: i == 0,
		})
	}
	for _, s := range raw.FileInfo.StreamDetails.Subtitle {
		d.Streams = append(d.Streams, Stream{
			Type: "Subtitle", Codec: strings.ToLower(text(s.Codec)),
			Language: defaultLang(text(s.Language)), Title: text(s.Title),
			Forced: strings.EqualFold(text(s.Forced), "true"),
		})
	}
	return d, nil
}

// Overview 返回优先使用的简介。
func (d *Data) Overview() string {
	if strings.TrimSpace(d.Plot) != "" {
		return d.Plot
	}
	return d.Outline
}

// RuntimeSeconds 返回秒级时长（NFO 用分钟）。
func (d *Data) RuntimeSeconds() int { return d.RuntimeMin * 60 }

func defaultLang(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "und"
	}
	return s
}

func normalizeDate(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Kodi 允许 2023、2023-05、2023-05-01；Emby 统一 YYYY-MM-DD。
	parts := strings.SplitN(s, "-", 3)
	for len(parts) < 3 {
		parts = append(parts, "01")
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil || y <= 0 {
		return ""
	}
	mo := atoi(parts[1])
	if mo <= 0 || mo > 12 {
		mo = 1
	}
	day := atoi(parts[2])
	if day <= 0 || day > 31 {
		day = 1
	}
	return pad4(y) + "-" + pad2(mo) + "-" + pad2(day)
}

func pad2(n int) string {
	if n < 10 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

func pad4(n int) string {
	s := strconv.Itoa(n)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func text(v string) string { return strings.TrimSpace(v) }

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

func atof(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

// ---- XML 结构 ----

type root struct {
	Title         string `xml:"title"`
	OriginalTitle string `xml:"originaltitle"`
	SortTitle     string `xml:"sorttitle"`
	Outline       string `xml:"outline"`
	Plot          string `xml:"plot"`
	Tagline       string `xml:"tagline"`
	Year          string `xml:"year"`
	Premiered     string `xml:"premiered"`
	Rating        string `xml:"rating"`
	CriticRating  string `xml:"criticrating"`
	Runtime       string `xml:"runtime"`
	MPAA          string `xml:"mpaa"`
	Season        string `xml:"season"`
	Episode       string `xml:"episode"`
	ShowTitle     string `xml:"showtitle"`

	GenreXML    []string   `xml:"genre"`
	TagXML      []string   `xml:"tag"`
	StudioXML   []string   `xml:"studio"`
	CreditXML   []string   `xml:"credits"`
	DirectorXML []string   `xml:"director"`
	Actor       []actorXML `xml:"actor"`
	UniqueID    []uidXML   `xml:"uniqueid"`
	FileInfo    fileInfo   `xml:"fileinfo"`

	// Kodi 的 <genre> 也可能包在 <genres> 里，双写兼容。
	GenresNode struct {
		Items []string `xml:"genre"`
	} `xml:"genres"`
	TagsNode struct {
		Items []string `xml:"tag"`
	} `xml:"tags"`
	StudiosNode struct {
		Items []string `xml:"studio"`
	} `xml:"studios"`
}

func (r *root) Genres() []string {
	return append(append([]string{}, r.GenreXML...), r.GenresNode.Items...)
}
func (r *root) Tags() []string { return append(append([]string{}, r.TagXML...), r.TagsNode.Items...) }
func (r *root) Studios() []string {
	return append(append([]string{}, r.StudioXML...), r.StudiosNode.Items...)
}
func (r *root) Credits() []string   { return r.CreditXML }
func (r *root) Directors() []string { return r.DirectorXML }

type actorXML struct {
	Name      string `xml:"name"`
	Role      string `xml:"role"`
	SortOrder string `xml:"sortorder"`
}

type uidXML struct {
	Type  string `xml:"type,attr"`
	Value string `xml:",chardata"`
}

type fileInfo struct {
	StreamDetails struct {
		Video    []videoXML    `xml:"video"`
		Audio    []audioXML    `xml:"audio"`
		Subtitle []subtitleXML `xml:"subtitle"`
	} `xml:"streamdetails"`
}

type videoXML struct {
	Codec  string `xml:"codec"`
	Width  string `xml:"width"`
	Height string `xml:"height"`
}

type audioXML struct {
	Codec    string `xml:"codec"`
	Channels string `xml:"channels"`
	Language string `xml:"language"`
	Title    string `xml:"title"`
}

type subtitleXML struct {
	Codec    string `xml:"codec"`
	Language string `xml:"language"`
	Title    string `xml:"title"`
	Forced   string `xml:"forced"`
}
