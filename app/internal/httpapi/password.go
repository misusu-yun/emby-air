package httpapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"strings"
)

// randomToken 生成 32 字节随机令牌的 hex 形式（64 字符）。
func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// tokenSessionID 由令牌派生稳定的会话 ID：不落库、可重现、不泄露令牌。
func tokenSessionID(token string) string {
	sum := sha256.Sum256([]byte("session:" + token))
	return hex.EncodeToString(sum[:16])
}

// verifyPasswordHash 校验密码哈希。
//
// 支持的存储形式（按前缀判定）：
//
//	sha256$<hex>         — 纯 SHA-256（种子脚本约定）
//	sha1$<hex>           — 纯 SHA-1
//	hmac-sha256$<hex>    — HMAC-SHA256(key=password, msg=<token>) 的十六进制摘要，
//	                        无法在不知明文的情况下逐次比对，故按 Emby 的 PBKDF2 变体跳过；
//	其它（含裸 hex）按 64 位 = sha256、40 位 = sha1 猜测。
//
// 另兼容 Emby 原生 <b64 salt>$<b64 pbkdf2> 形式：salt 与 pbkdf2 均为 base64。
func verifyPasswordHash(stored, password string) bool {
	if stored == "" {
		return false
	}
	lower := strings.ToLower(stored)

	switch {
	case strings.HasPrefix(lower, "sha256$"):
		return eqHex(sha256Hex(password), stored[len("sha256$"):])
	case strings.HasPrefix(lower, "sha1$"):
		return eqHex(sha1Hex(password), stored[len("sha1$"):])
	case strings.HasPrefix(lower, "sha256:"):
		return eqHex(sha256Hex(password), stored[len("sha256:"):])
	case strings.HasPrefix(lower, "sha1:"):
		return eqHex(sha1Hex(password), stored[len("sha1:"):])
	}

	pw, salt, ok := splitEmbyPBKDF2(stored)
	if ok {
		return eqHex(pw, hex.EncodeToString(embyPBKDF2(password, salt)))
	}

	// 裸 hex：按长度猜算法。
	switch len(stored) {
	case 64:
		return eqHex(sha256Hex(password), stored)
	case 40:
		return eqHex(sha1Hex(password), stored)
	}
	return false
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// eqHex 做常量时间比较，忽略大小写差异（先统一小写）。
func eqHex(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// splitEmbyPBKDF2 解析 "<base64salt>$<base64derived>" 形式，返回 hex 期望值与 salt。
func splitEmbyPBKDF2(stored string) (wantHex string, salt []byte, ok bool) {
	i := strings.IndexByte(stored, '$')
	if i <= 0 || i == len(stored)-1 {
		return "", nil, false
	}
	s, err := decodeB64(stored[:i])
	if err != nil || len(s) == 0 {
		return "", nil, false
	}
	d, err := decodeB64(stored[i+1:])
	if err != nil || len(d) == 0 {
		return "", nil, false
	}
	return hex.EncodeToString(d), s, true
}

func embyPBKDF2(password string, salt []byte) []byte {
	// Emby 使用 ASP.NET 的 Rfc2898DeriveBytes(SHA1, 1000, 32)。
	return pbkdf2SHA1([]byte(password), salt, 1000, 32)
}

// pbkdf2SHA1 是最小实现（单块输出足够 32 字节 = 1 块，但按通用形式写）。
func pbkdf2SHA1(password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(sha1.New, password)
	out := make([]byte, 0, keyLen)
	var counter [4]byte
	for block := 1; len(out) < keyLen; block++ {
		counter[0] = byte(block >> 24)
		counter[1] = byte(block >> 16)
		counter[2] = byte(block >> 8)
		counter[3] = byte(block)
		var u hash.Hash = prf
		u.Reset()
		u.Write(salt)
		u.Write(counter[:])
		sum := u.Sum(nil)
		t := append([]byte(nil), sum...)
		for range iter - 1 {
			u.Reset()
			u.Write(sum)
			sum = u.Sum(nil)
			for k := range t {
				t[k] ^= sum[k]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func decodeB64(s string) ([]byte, error) {
	// Emby 使用标准 base64（可省略 padding）；同时容忍 URL-safe 变体。
	enc := base64.RawStdEncoding
	if strings.HasSuffix(s, "=") {
		enc = base64.StdEncoding
	}
	if b, err := enc.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}
