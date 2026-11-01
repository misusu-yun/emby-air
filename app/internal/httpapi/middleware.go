package httpapi

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// CONTRACT_LOG 环境变量指向 JSONL 日志文件；未设置则完全不记录。
const contractLogEnv = "CONTRACT_LOG"

// logChunk 是一条待写入的日志行（已编码，含换行）。
type logChunk []byte

// handleRecorder 捕获响应状态码，供访问日志使用。
type handleRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (h *handleRecorder) WriteHeader(code int) {
	if !h.wrote {
		h.status = code
		h.wrote = true
	}
	h.ResponseWriter.WriteHeader(code)
}

func (h *handleRecorder) Write(b []byte) (int, error) {
	if !h.wrote {
		h.status = http.StatusOK
		h.wrote = true
	}
	return h.ResponseWriter.Write(b)
}

// Unwrap 让 http.ResponseController 能拿到底层 Conn（Flush/Hijack 等）。
func (h *handleRecorder) Unwrap() http.ResponseWriter { return h.ResponseWriter }

// Flush 转发到底层，SSE 才能实时推送。
func (h *handleRecorder) Flush() {
	if f, ok := h.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// logRequests 是访问日志中间件：JSONL 追加到 CONTRACT_LOG。
// 只记录元数据（ts/client/method/path/query_keys/status/dur_ms），
// 绝不记录 body、令牌或 query 值。
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.logQ == nil {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &handleRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		dur := time.Since(start)

		keys := queryKeys(r.URL.RawQuery)
		line, err := json.Marshal(map[string]any{
			"ts":          start.UTC().Format(time.RFC3339Nano),
			"client":      clientAddr(r),
			"method":      r.Method,
			"path":        r.URL.Path,
			"query_keys":  keys,
			"status":      rec.status,
			"dur_ms":      float64(dur.Nanoseconds()) / 1e6,
			"ua":          truncateRunes(r.UserAgent(), 90),
			"emby_client": truncateRunes(r.Header.Get("X-Emby-Client"), 40),
		})
		if err != nil {
			return
		}
		line = append(line, '\n')
		select {
		case s.logQ <- line:
		default:
			// 通道满时丢弃日志，绝不阻塞请求。
		}
	})
}

// queryKeys 只返回参数名（排序去重），值可能含敏感信息。
func queryKeys(raw string) []string {
	if raw == "" {
		return []string{}
	}
	set := make(map[string]struct{}, 8)
	for _, kv := range strings.Split(raw, "&") {
		if kv == "" {
			continue
		}
		k := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			k = kv[:i]
		}
		if k == "" {
			continue
		}
		set[strings.ToLower(k)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// clientAddr 取远端 IP（去掉端口），不信任 X-Forwarded-For。
func clientAddr(r *http.Request) string {
	if r.RemoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// startLogWriter 打开 CONTRACT_LOG 并启动单后台 goroutine 串行追加。
// 多次调用安全（已启动则直接返回）。
func (s *Server) startLogWriter() {
	s.logOnce.Do(func() {
		path := strings.TrimSpace(os.Getenv(contractLogEnv))
		if path == "" {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		q := make(chan logChunk, 1024)
		s.logQ = q
		s.logWG.Add(1)
		go func() {
			defer s.logWG.Done()
			bw := bufio.NewWriterSize(f, 32<<10)
			for line := range q {
				if _, err := bw.Write(line); err != nil {
					return
				}
				// 通道空则立刻落盘，避免丢尾部日志。
				if len(q) == 0 {
					_ = bw.Flush()
				}
			}
			_ = bw.Flush()
			_ = f.Close()
		}()
	})
}

// closeLogWriter 关闭日志通道（仅测试使用；生产进程随退出结束）。
func (s *Server) closeLogWriter() {
	if s.logQ != nil {
		close(s.logQ)
		s.logQ = nil
		// 等 goroutine 把缓冲区刷完，调用方才能立刻读到文件内容。
		s.logWG.Wait()
	}
}
