package httpapi

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"strings"

	"emby-air/internal/model"
)

// handleMoonfinCompat 处理 Moonfin 的私有扩展接口：不实现具体功能，但返回客户端能接受的空结果，
// 同时避免落到兼容兜底里刷日志。
func (s *Server) handleMoonfinCompat(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// handleItemAncestors 对应 GET /emby/Items/{Id}/Ancestors。
// 返回父级链（季 → 剧 → 库），Moonfin 用它显示“所属剧集/媒体库”。
func (s *Server) handleItemAncestors(w http.ResponseWriter, r *http.Request) {
	if s.db == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	id := r.PathValue("Id")
	if !s.itemAllowed(r.Context(), userFrom(r), id) {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	out := make([]model.Item, 0, 4)
	seen := map[string]bool{}
	for i := 0; i < 5 && id != "" && !seen[id]; i++ {
		seen[id] = true
		it, err := s.db.GetItem(r.Context(), id)
		if err != nil || it == nil {
			break
		}
		out = append(out, *it)
		id = it.ParentId
	}
	writeJSON(w, http.StatusOK, out)
}

// handleEmbyWebSocket 做最小可用的 WebSocket 握手并保持连接（只回 pong），
// 避免 Moonfin 反复重连、把日志刷满。
func (s *Server) handleEmbyWebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		w.WriteHeader(http.StatusOK)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing websocket key")
		return
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])

	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot upgrade")
		return
	}
	defer conn.Close()

	_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
	_ = brw.Flush()

	// 只处理 ping/close：pong 回包，close 退出。其它帧忽略。
	rd := bufio.NewReader(conn)
	for {
		op, payload, err := readWSFrame(rd)
		if err != nil {
			return
		}
		switch op {
		case 0x8: // close
			return
		case 0x9: // ping -> pong
			writeWSFrame(brw.Writer, 0xA, payload)
		}
	}
}

func readWSFrame(r *bufio.Reader) (byte, []byte, error) {
	h, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	op := h & 0x0f
	b2, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	masked := b2&0x80 != 0
	n := int(b2 & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := r.Read(ext[:]); err != nil {
			return 0, nil, err
		}
		n = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := r.Read(ext[:]); err != nil {
			return 0, nil, err
		}
		n = int(binary.BigEndian.Uint64(ext[:]))
	}
	var mask [4]byte
	if masked {
		if _, err := r.Read(mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := r.Read(payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return op, payload, nil
}

func writeWSFrame(w *bufio.Writer, op byte, payload []byte) {
	_ = w.WriteByte(0x80 | op)
	if len(payload) < 126 {
		_ = w.WriteByte(byte(len(payload)))
	} else {
		_ = w.WriteByte(126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(len(payload)))
		_, _ = w.Write(ext[:])
	}
	_, _ = w.Write(payload)
	_ = w.Flush()
}
