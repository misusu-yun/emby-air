// Package settings 保存可在 Web 管理端修改的运行时设置。
// 数据库中的值优先；没有设置时回退到环境变量，方便 Docker 首次启动。
package settings

import (
	"context"
	"os"
	"strconv"
	"strings"

	"emby-air/internal/store"
)

const (
	KeyTMDBAPIKey = "tmdb_api_key"
	KeyTMDBLang   = "tmdb_lang"
	KeyProxy      = "proxy"
	KeyNoProxy    = "no_proxy"
	KeyScrapeRate = "scrape_rate"
	KeyAutoScrape = "auto_scrape"
	KeyNFOWrite   = "nfo_write"
)

// Settings 是当前生效的刮削/TMDB 配置。
type Settings struct {
	TMDBAPIKey string
	TMDBLang   string
	Proxy      string
	NoProxy    string
	ScrapeRate int
	AutoScrape bool
	NFOWrite   bool
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func envBool(keys []string, def bool) bool {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") || strings.EqualFold(v, "on")
		}
	}
	return def
}

// Load 读取数据库设置；数据库没有的项用环境变量兜底。
func Load(ctx context.Context, db *store.DB) Settings {
	s := Settings{
		TMDBLang:   env("TMDB_LANG", "zh-CN"),
		ScrapeRate: 0,
		AutoScrape: true,
		NFOWrite:   false,
	}
	has := func(k string) bool { return db != nil && db.HasSetting(ctx, k) }
	if db != nil {
		if has(KeyTMDBAPIKey) {
			s.TMDBAPIKey = strings.TrimSpace(db.GetSetting(ctx, KeyTMDBAPIKey))
		}
		if has(KeyTMDBLang) {
			s.TMDBLang = strings.TrimSpace(db.GetSetting(ctx, KeyTMDBLang))
		}
		if has(KeyProxy) {
			s.Proxy = strings.TrimSpace(db.GetSetting(ctx, KeyProxy))
		}
		if has(KeyNoProxy) {
			s.NoProxy = strings.TrimSpace(db.GetSetting(ctx, KeyNoProxy))
		}
		if has(KeyScrapeRate) {
			if n, err := strconv.Atoi(strings.TrimSpace(db.GetSetting(ctx, KeyScrapeRate))); err == nil {
				s.ScrapeRate = n
			}
		}
		if has(KeyAutoScrape) {
			s.AutoScrape = db.GetSetting(ctx, KeyAutoScrape) != "0"
		}
		if has(KeyNFOWrite) {
			s.NFOWrite = db.GetSetting(ctx, KeyNFOWrite) == "1"
		}
	}
	if s.TMDBAPIKey == "" {
		s.TMDBAPIKey = env("TMDB_API_KEY", "")
	}
	if s.TMDBLang == "" {
		s.TMDBLang = env("TMDB_LANG", "zh-CN")
	}
	if s.Proxy == "" {
		s.Proxy = env("HTTPS_PROXY", env("HTTP_PROXY", ""))
	}
	if s.NoProxy == "" {
		s.NoProxy = env("NO_PROXY", "")
	}
	if s.ScrapeRate <= 0 {
		if n, err := strconv.Atoi(env("SCRAPE_RATE", "")); err == nil {
			s.ScrapeRate = n
		}
	}
	if s.ScrapeRate <= 0 {
		s.ScrapeRate = 15
	}
	if s.ScrapeRate < 1 {
		s.ScrapeRate = 1
	}
	if s.ScrapeRate > 15 {
		s.ScrapeRate = 15
	}
	// 环境变量作为首次启动的默认值；数据库显式设置优先。
	if db == nil {
		s.AutoScrape = envBool([]string{"AUTO_SCRAPE"}, s.AutoScrape)
		s.NFOWrite = envBool([]string{"NFO_ENABLED", "NFO_WRITE"}, s.NFOWrite)
	} else {
		if !db.HasSetting(ctx, KeyAutoScrape) {
			s.AutoScrape = envBool([]string{"AUTO_SCRAPE"}, s.AutoScrape)
		}
		if !db.HasSetting(ctx, KeyNFOWrite) {
			s.NFOWrite = envBool([]string{"NFO_ENABLED", "NFO_WRITE"}, s.NFOWrite)
		}
	}
	return s
}

// Save 写入数据库，并保证速率在安全范围内。
func Save(ctx context.Context, db *store.DB, s Settings) error {
	if s.ScrapeRate < 1 {
		s.ScrapeRate = 1
	}
	if s.ScrapeRate > 15 {
		s.ScrapeRate = 15
	}
	if strings.TrimSpace(s.TMDBLang) == "" {
		s.TMDBLang = "zh-CN"
	}
	auto := "0"
	if s.AutoScrape {
		auto = "1"
	}
	nfo := "0"
	if s.NFOWrite {
		nfo = "1"
	}
	values := map[string]string{
		KeyTMDBAPIKey: strings.TrimSpace(s.TMDBAPIKey),
		KeyTMDBLang:   strings.TrimSpace(s.TMDBLang),
		KeyProxy:      strings.TrimSpace(s.Proxy),
		KeyNoProxy:    strings.TrimSpace(s.NoProxy),
		KeyScrapeRate: strconv.Itoa(s.ScrapeRate),
		KeyAutoScrape: auto,
		KeyNFOWrite:   nfo,
	}
	for k, v := range values {
		if err := db.SetSetting(ctx, k, v); err != nil {
			return err
		}
	}
	return nil
}
