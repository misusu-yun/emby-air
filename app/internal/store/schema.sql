-- schema v1
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA cache_size=-64000;
PRAGMA mmap_size=268435456;

CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,              -- Movie|Series|Season|Episode|Folder|Person
  name TEXT NOT NULL,
  original_title TEXT,
  sort_name TEXT NOT NULL,
  forced_sort_name TEXT,
  parent_id TEXT NOT NULL DEFAULT '',
  series_id TEXT NOT NULL DEFAULT '',
  series_name TEXT NOT NULL DEFAULT '',
  season_id TEXT NOT NULL DEFAULT '',
  season_number INTEGER DEFAULT 0,
  episode_number INTEGER DEFAULT 0,
  year INTEGER DEFAULT 0,
  runtime_ticks INTEGER DEFAULT 0,
  overview TEXT,
  official_rating TEXT,
  community_rating REAL DEFAULT 0,
  critic_rating REAL DEFAULT 0,
  path TEXT,
  container TEXT,
  tmdb_id TEXT,
  is_folder INTEGER DEFAULT 0,
  genres TEXT,                     -- JSON array
  image_tags TEXT,                 -- JSON object {"Primary":"hash"}
  backdrop_tags TEXT,              -- JSON array
  provider_ids TEXT,               -- JSON object
  date_created INTEGER NOT NULL,
  sort_index INTEGER DEFAULT 0,
  tmdb_still TEXT DEFAULT '',        -- 单集缩略图（TMDB still_path），按需下载
  pinyin TEXT DEFAULT '',            -- 标题全拼（小写，无空格）
  pinyin_initials TEXT DEFAULT ''    -- 标题首字母（小写）
);

-- '' 而非 NULL：索引与比较都能命中，避免 NULL 语义泄漏到查询层
CREATE INDEX IF NOT EXISTS idx_items_parent ON items(parent_id);
CREATE INDEX IF NOT EXISTS idx_items_series_id ON items(series_id);

CREATE TABLE IF NOT EXISTS item_paths (
  path TEXT PRIMARY KEY,
  item_id TEXT NOT NULL,
  mtime INTEGER NOT NULL,
  size INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_paths_item ON item_paths(item_id);

CREATE TABLE IF NOT EXISTS views (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  collection_type TEXT,
  path TEXT,
  paths TEXT DEFAULT '[]',
  sort_order INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,       -- 由 Open 设为 NOCASE，登录大小写不敏感
  password_hash TEXT NOT NULL,
  is_admin INTEGER DEFAULT 0,
  created INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tokens (
  token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  device_id TEXT,
  client TEXT,
  created INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  kind TEXT NOT NULL DEFAULT 'client',      -- client | web | apikey
  client_version TEXT DEFAULT '',
  device_name TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tokens_user ON tokens(user_id);

CREATE TABLE IF NOT EXISTS user_data (
  user_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  played INTEGER DEFAULT 0,
  play_count INTEGER DEFAULT 0,
  position_ticks INTEGER DEFAULT 0,
  is_favorite INTEGER DEFAULT 0,
  last_played INTEGER,
  duration_ticks INTEGER DEFAULT 0,
  updated INTEGER DEFAULT 0,
  PRIMARY KEY (user_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_userdata_resume ON user_data(user_id, position_ticks DESC);
CREATE INDEX IF NOT EXISTS idx_userdata_played ON user_data(user_id, played);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT,
  device_id TEXT,
  client TEXT,
  item_id TEXT,
  position_ticks INTEGER DEFAULT 0,
  is_paused INTEGER DEFAULT 0,
  updated INTEGER NOT NULL,
  device_name TEXT DEFAULT '',
  user_name TEXT DEFAULT '',
  item_name TEXT DEFAULT '',
  runtime_ticks INTEGER DEFAULT 0,
  started INTEGER DEFAULT 0,
  client_version TEXT DEFAULT '',
  play_method TEXT DEFAULT '',
  ip TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions(updated DESC);

-- 活动流：登录、开始/停止播放、播放完成、删除、管理操作。
CREATE TABLE IF NOT EXISTS activities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  user_id TEXT NOT NULL DEFAULT '',
  user_name TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  item_id TEXT NOT NULL DEFAULT '',
  item_name TEXT NOT NULL DEFAULT '',
  client TEXT NOT NULL DEFAULT '',
  device TEXT NOT NULL DEFAULT '',
  ip TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_activities_ts ON activities(ts DESC);

-- 设备指纹：X-Emby-Device-Id（弱指纹以 weak: 开头）。
CREATE TABLE IF NOT EXISTS devices (
  device_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  client TEXT NOT NULL DEFAULT '',
  device_name TEXT NOT NULL DEFAULT '',
  version TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT 'unknown',
  ip TEXT NOT NULL DEFAULT '',
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS settings (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

-- 排序索引：必须覆盖热路径，避免临时排序树。
-- 每个排序方向都有对应索引，keyset 才能双向走索引。
CREATE INDEX IF NOT EXISTS idx_items_sort_name      ON items(sort_name, id);
CREATE INDEX IF NOT EXISTS idx_items_sort_name_desc ON items(sort_name DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_items_created        ON items(date_created DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_items_created_asc    ON items(date_created, id);
CREATE INDEX IF NOT EXISTS idx_items_year           ON items(year);
-- 父级 + 排序（Recursive=false 且带排序）
CREATE INDEX IF NOT EXISTS idx_items_parent_sort    ON items(parent_id, sort_name, id);
CREATE INDEX IF NOT EXISTS idx_items_parent_created ON items(parent_id, date_created DESC, id DESC);
-- kind + 排序（按类型过滤时）
CREATE INDEX IF NOT EXISTS idx_items_kind_sort      ON items(kind, sort_name, id);
CREATE INDEX IF NOT EXISTS idx_items_kind_created   ON items(kind, date_created DESC, id DESC);
-- 剧集内集序
CREATE INDEX IF NOT EXISTS idx_items_series         ON items(series_id, season_number, episode_number);
CREATE INDEX IF NOT EXISTS idx_items_year_sort      ON items(year, sort_name, id);

-- FTS5 外部内容表：内容由触发器维护，避免双写不一致。
-- unicode61 分词（原生支持，不依赖 trigram 模块）；查询走子串匹配。
CREATE VIRTUAL TABLE IF NOT EXISTS items_fts USING fts5(
  name, original_title, series_name, overview,
  content='items', content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS items_fts_ai AFTER INSERT ON items BEGIN
  INSERT INTO items_fts(rowid, name, original_title, series_name, overview)
  VALUES (new.rowid, new.name, COALESCE(new.original_title,''), new.series_name, COALESCE(new.overview,''));
END;

CREATE TRIGGER IF NOT EXISTS items_fts_ad AFTER DELETE ON items BEGIN
  INSERT INTO items_fts(items_fts, rowid, name, original_title, series_name, overview)
  VALUES ('delete', old.rowid, old.name, COALESCE(old.original_title,''), old.series_name, COALESCE(old.overview,''));
END;

CREATE TRIGGER IF NOT EXISTS items_fts_au AFTER UPDATE OF name, original_title, series_name, overview ON items BEGIN
  INSERT INTO items_fts(items_fts, rowid, name, original_title, series_name, overview)
  VALUES ('delete', old.rowid, old.name, COALESCE(old.original_title,''), old.series_name, COALESCE(old.overview,''));
  INSERT INTO items_fts(rowid, name, original_title, series_name, overview)
  VALUES (new.rowid, new.name, COALESCE(new.original_title,''), new.series_name, COALESCE(new.overview,''));
END;

-- ---- schema v2：刮削元数据 ----
-- items 扩列用 ALTER 完成（见 migrations），新库由 migrateV2 在 CREATE 之后补齐。
-- 这里只声明新表；列由 store.migrate 统一处理，避免同一份 DDL 写两遍。

-- scrape_state 记录每个条目最近一次刮削的结果，用于断点续跑与失败隔离。
--   status: ok | notfound | failed | skipped
--   source: tmdb | nfo | local
CREATE TABLE IF NOT EXISTS scrape_state (
  item_id   TEXT PRIMARY KEY,
  status    TEXT NOT NULL,
  source    TEXT NOT NULL DEFAULT '',
  attempts  INTEGER NOT NULL DEFAULT 0,
  error     TEXT NOT NULL DEFAULT '',
  updated   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scrape_status ON scrape_state(status);

-- 一个条目可以有多个版本（DHR/杜比/Other…），每个版本一条来源。
CREATE TABLE IF NOT EXISTS media_sources (
  item_id       TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  name          TEXT NOT NULL DEFAULT '',
  path          TEXT NOT NULL,
  container     TEXT DEFAULT 'strm',
  runtime_ticks INTEGER DEFAULT 0,
  size          INTEGER DEFAULT 0,
  resolved_url  TEXT DEFAULT '',   -- strm 内直链缓存：避免每次详情/播放都读磁盘
  sort_order    INTEGER DEFAULT 0,
  PRIMARY KEY (item_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_media_sources_item ON media_sources(item_id);

-- API Key：Emby 风格的长期凭据（32 位 hex），独立于用户 token。
CREATE TABLE IF NOT EXISTS api_keys (
  key     TEXT PRIMARY KEY,
  note    TEXT DEFAULT '',
  created INTEGER NOT NULL
);

-- media_probes 缓存上游媒体探测结果（大小、ETag、音视频/字幕轨），
-- 避免每次打开详情页都重新 ffprobe；URL/ETag/大小变化时由服务端刷新。
CREATE TABLE IF NOT EXISTS media_probes (
  item_id       TEXT NOT NULL,
  source_id     TEXT NOT NULL,
  target        TEXT NOT NULL DEFAULT '',
  size          INTEGER NOT NULL DEFAULT 0,
  etag          TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL DEFAULT '',
  tracks        TEXT NOT NULL DEFAULT '[]',
  error         TEXT NOT NULL DEFAULT '',
  probed_at     INTEGER NOT NULL DEFAULT 0,
  checked_at    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (item_id, source_id)
);
CREATE INDEX IF NOT EXISTS idx_media_probes_item ON media_probes(item_id);
