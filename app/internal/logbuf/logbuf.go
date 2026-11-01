// Package logbuf 保存最近 N 行日志，供 Web 面板查看（不落盘、不无限增长）。
package logbuf

import (
	"strings"
	"sync"
)

// Buffer 是固定容量的环形日志缓冲。
type Buffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

// New 创建最多保留 n 行的缓冲。
func New(n int) *Buffer {
	if n <= 0 {
		n = 500
	}
	return &Buffer{cap: n, lines: make([]string, 0, n)}
}

// Write 实现 io.Writer，按行切分后存入。
func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line == "" {
			continue
		}
		b.lines = append(b.lines, line)
		if len(b.lines) > b.cap {
			b.lines = b.lines[len(b.lines)-b.cap:]
		}
	}
	return len(p), nil
}

// Tail 返回最近 limit 行（limit<=0 取全部）。
func (b *Buffer) Tail(limit int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 || limit > len(b.lines) {
		limit = len(b.lines)
	}
	out := make([]string, limit)
	copy(out, b.lines[len(b.lines)-limit:])
	return out
}
