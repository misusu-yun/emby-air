# emby-air Docker 测试版

> **AI 开发说明：** 本项目包含 AI 辅助生成/修改的代码和文档，可能存在错误或安全风险；使用前请自行审查和测试。详见 [AI-DISCLOSURE.md](AI-DISCLOSURE.md)。
>
> **非官方项目：** 与 Emby LLC、TMDB、Pansave 无隶属或背书关系。使用者需自行确保媒体内容和第三方服务的使用合法。详见 [LEGAL.md](LEGAL.md)。

类 Emby 的轻量媒体服务器，支持扫描 `.strm`、TMDB 刮削、Web 管理和常见 Emby 客户端。

## 快速开始

```bash
cp .env.example .env
# 编辑 .env：至少设置 MEDIA_PATH、DATA_PATH、ADMIN_PASSWORD
docker compose up -d --build
```

打开 `http://<宿主机IP>:8098/webui`，用 `.env` 里的管理员账号登录。

更完整的步骤见 [USAGE.html](USAGE.html)。

## 功能

- 扫描 `.strm` / 视频文件，按 `{tmdb-xxx}` 或年份识别影片与剧集
- TMDB 刮削：海报、背景、Logo、简介、演职员、分集名称/剧照
- 分集缩略图按需下载，不会一次性占满磁盘
- 多版本合并、同集去重、拼音/首字母搜索
- 播放进度、继续观看、正在播放、活动记录
- Web 管理端：媒体库、影片管理、用户与库权限、任务、日志
- Web 设置页：TMDB Key、代理、刮削速率、自动刮削、NFO 开关

## 文件说明

| 文件 | 说明 |
|---|---|
| `Dockerfile` | 多阶段构建，运行时只有单个二进制 |
| `docker-compose.yml` | 端口、卷、环境变量、健康检查 |
| `.env.example` | 配置模板，复制为 `.env` 后修改 |
| `app/` | Go 源码（不含测试和开发工具） |
| `USAGE.html` | 详细使用教程 |
| `AI-DISCLOSURE.md` | AI 开发生成说明 |
| `LEGAL.md` | 法律与使用声明 |
| `LICENSE` | MIT License |

## 常用命令

```bash
docker compose logs -f --tail=100
docker compose restart
docker compose down
docker compose build --no-cache
```

## 配置说明

- 环境变量是首次启动默认值；
- Web「设置」页保存的 TMDB Key、代理、刮削速率、自动刮削、NFO 开关会持久化到 `data/emby.db`，优先级高于环境变量；
- 修改 Web 设置后新任务立即生效，正在运行的任务沿用旧配置。

## 字幕

- 支持与 `.strm` 同目录的外挂字幕：`.srt`、`.ass`、`.vtt`；
- 也支持 `影片.zh.srt`、`影片.chs.srt`、`影片.en.srt` 等语言后缀；
- 内嵌字幕通过 `ffprobe` 探测并作为标准 `MediaStream` 下发，交给支持内嵌轨道的播放器直接读取；
- Docker 镜像已包含 `ffmpeg/ffprobe`。

bridge 网络下如果 `.strm` 使用 `127.0.0.1` 指向宿主机服务，请在 `.env` 设置：

```text
UPSTREAM_HOST=host.docker.internal
```

并保留 compose 中的 `host.docker.internal:host-gateway`。如果使用 host 网络，可以留空。

## 数据与安全

- 备份 `data/` 目录前先停止容器；
- 不要把 `.env`、`data/`、API Key 或管理员密码发到公开仓库；
- 不要把 8098 直接暴露到公网，建议使用反向代理、VPN 或内网访问；
- 首次登录后立即修改管理员密码。
