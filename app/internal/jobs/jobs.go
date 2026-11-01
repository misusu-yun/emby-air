// Package jobs 提供进程内的串行任务队列（扫描 / 刮削），带进度与状态快照。
// 单 worker 串行执行：SQLite 单写者，串行能避免任务互相抢锁。
package jobs

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Kind 任务类型。
type Kind string

const (
	KindScan    Kind = "scan"
	KindScrape  Kind = "scrape"
	KindRescan  Kind = "rescan"  // 单条重新扫描
	KindRescore Kind = "rescore" // 单条重新刮削
)

// Mode 执行模式。
type Mode string

const (
	ModeIncremental Mode = "incremental"
	ModeFull        Mode = "full"
)

// State 任务状态。
type State string

const (
	StateQueued   State = "queued"
	StateRunning  State = "running"
	StateDone     State = "done"
	StateFailed   State = "failed"
	StateCanceled State = "canceled"
)

// Job 是一轮扫描或刮削的运行时快照。
type Job struct {
	Id          string `json:"Id"`
	Kind        Kind   `json:"Kind"`
	Mode        Mode   `json:"Mode"`
	LibraryId   string `json:"LibraryId,omitempty"`
	ItemId      string `json:"ItemId,omitempty"`
	LibraryName string `json:"LibraryName,omitempty"`
	// Paths 是 Pansave 定向通知带来的具体目录；非空时扫描/刮削只处理这些路径。
	Paths    []string `json:"-"`
	State    State    `json:"State"`
	Created  int64    `json:"Created"`
	Started  int64    `json:"Started"`
	Finished int64    `json:"Finished"`
	Total    int      `json:"Total"`
	Done     int      `json:"Done"`
	Message  string   `json:"Message"`
	Error    string   `json:"Error"`
}

// Manager 串行执行任务，保留最近 finishedCap 条完成记录。
type Manager struct {
	mu       sync.Mutex
	queue    []*Job
	running  *Job
	finished []*Job
	seq      atomic.Int64
	run      func(context.Context, *Job) error
	// cancel 是当前运行任务的取消函数；空闲时为 nil。
	cancel context.CancelFunc
	wake   chan struct{}
}

const finishedCap = 50

// New 创建管理器并启动 worker。
func New(run func(context.Context, *Job) error) *Manager {
	m := &Manager{run: run, wake: make(chan struct{}, 1)}
	go m.loop()
	return m
}

// SubmitItem 入队一个针对单条媒体的任务（重新扫描/重新刮削）。
func (m *Manager) SubmitItem(kind Kind, itemID, name string) *Job {
	j := &Job{
		Id:          strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(m.seq.Add(1), 10),
		Kind:        kind,
		ItemId:      itemID,
		LibraryName: name,
		State:       StateQueued,
		Created:     time.Now().Unix(),
	}
	m.mu.Lock()
	m.queue = append(m.queue, j)
	m.mu.Unlock()
	log.Printf("任务入队：[%s] %s", kind, name)
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return j
}

// Submit 入队一个任务。
func (m *Manager) Submit(kind Kind, mode Mode, libraryID, libraryName string) *Job {
	return m.submit(&Job{
		Kind:        kind,
		Mode:        mode,
		LibraryId:   libraryID,
		LibraryName: libraryName,
	})
}

// SubmitPaths 入队一个只处理指定目录的任务（Pansave 定向刷新）。
func (m *Manager) SubmitPaths(kind Kind, mode Mode, paths []string, name string) *Job {
	cp := append([]string(nil), paths...)
	return m.submit(&Job{
		Kind:        kind,
		Mode:        mode,
		LibraryName: name,
		Paths:       cp,
	})
}

func (m *Manager) submit(j *Job) *Job {
	j.Id = strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(m.seq.Add(1), 10)
	j.State = StateQueued
	j.Created = time.Now().Unix()
	m.mu.Lock()
	m.queue = append(m.queue, j)
	m.mu.Unlock()
	if len(j.Paths) > 0 {
		log.Printf("任务入队：[%s/%s] %s（%d 个路径）", j.Kind, j.Mode, j.LibraryName, len(j.Paths))
	} else {
		log.Printf("任务入队：[%s/%s] %s", j.Kind, j.Mode, j.LibraryName)
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return j
}

// Update 更新进度（按任务 ID 匹配当前运行任务）。
func (m *Manager) Update(id string, total, done int, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running == nil || m.running.Id != id {
		return
	}
	if total >= 0 {
		m.running.Total = total
	}
	if done >= 0 {
		m.running.Done = done
	}
	if message != "" {
		m.running.Message = message
	}
}

// Snapshot 返回 运行中 / 排队中 / 已完成 三组快照（已完成按时间倒序）。
func (m *Manager) Snapshot() (running *Job, queued []*Job, done []*Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running != nil {
		cp := *m.running
		running = &cp
	}
	queued = make([]*Job, len(m.queue))
	for i, j := range m.queue {
		cp := *j
		queued[i] = &cp
	}
	done = make([]*Job, len(m.finished))
	for i, j := range m.finished {
		cp := *j
		done[i] = &cp
	}
	return
}

// Busy 表示是否有任务在跑或排队。
func (m *Manager) Busy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running != nil || len(m.queue) > 0
}

func (m *Manager) loop() {
	for range m.wake {
		for {
			m.mu.Lock()
			if m.running != nil || len(m.queue) == 0 {
				m.mu.Unlock()
				break
			}
			j := m.queue[0]
			m.queue = m.queue[1:]
			j.State = StateRunning
			j.Started = time.Now().Unix()
			ctx, cancel := context.WithCancel(context.Background())
			m.running = j
			m.cancel = cancel
			m.mu.Unlock()
			if j.Mode != "" {
				log.Printf("任务开始：[%s/%s] %s", j.Kind, j.Mode, j.LibraryName)
			} else {
				log.Printf("任务开始：[%s] %s", j.Kind, j.LibraryName)
			}

			err := m.run(ctx, j)
			cancel()
			m.mu.Lock()
			m.cancel = nil
			m.mu.Unlock()

			m.mu.Lock()
			j.Finished = time.Now().Unix()
			switch {
			case errors.Is(err, context.Canceled):
				j.State = StateCanceled
				j.Message = "已取消"
				log.Printf("任务已取消：[%s/%s] %s", j.Kind, j.Mode, j.LibraryName)
			case err != nil:
				j.State = StateFailed
				j.Error = err.Error()
				log.Printf("任务失败：[%s/%s] %s：%v", j.Kind, j.Mode, j.LibraryName, err)
			default:
				j.State = StateDone
				log.Printf("任务结束：[%s] %s，用时 %ds", j.Kind, j.LibraryName, j.Finished-j.Started)
			}
			m.running = nil
			m.finished = append([]*Job{j}, m.finished...)
			if len(m.finished) > finishedCap {
				m.finished = m.finished[:finishedCap]
			}
			m.mu.Unlock()
		}
	}
}

// Cancel 取消排队中或运行中的任务。
// 返回 true 表示已受理；扫描/刮削只写数据库与图片缓存，
// 取消发生在事务边界内，不会删除或损坏已有的媒体文件。
func (m *Manager) Cancel(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 排队中：直接移出队列并记入完成列表。
	for i, j := range m.queue {
		if j.Id != id {
			continue
		}
		j.State = StateCanceled
		j.Finished = time.Now().Unix()
		j.Message = "已取消"
		m.queue = append(m.queue[:i], m.queue[i+1:]...)
		m.finished = append([]*Job{j}, m.finished...)
		if len(m.finished) > finishedCap {
			m.finished = m.finished[:finishedCap]
		}
		log.Printf("任务已取消（排队中）：[%s/%s] %s", j.Kind, j.Mode, j.LibraryName)
		return true
	}
	// 运行中：触发 context 取消，worker 会落到 StateCanceled。
	if m.running != nil && m.running.Id == id && m.cancel != nil {
		m.cancel()
		return true
	}
	return false
}
