package store

import (
	"context"
	"encoding/json"
	"strings"
)

// UserPolicyPatch 是“只更新出现字段”的用户策略补丁。
// nil 表示该字段未提交，必须保留原值；空切片表示明确清空。
type UserPolicyPatch struct {
	IsAdministrator   *bool     `json:"IsAdministrator"`
	IsDisabled        *bool     `json:"IsDisabled"`
	MaxActiveSessions *int      `json:"MaxActiveSessions"`
	EnableAllFolders  *bool     `json:"EnableAllFolders"`
	EnabledFolders    *[]string `json:"EnabledFolders"`
	BlockedTags       *[]string `json:"BlockedTags"`
}

// UpdateUserPolicyPatch 只写 patch 中出现的列，避免 Web 端部分更新把
// IsAdministrator/IsDisabled/MaxActiveSessions 等未提交字段清零。
func (d *DB) UpdateUserPolicyPatch(ctx context.Context, id string, p UserPolicyPatch) error {
	sets := make([]string, 0, 6)
	args := make([]any, 0, 7)
	if p.IsAdministrator != nil {
		v := 0
		if *p.IsAdministrator {
			v = 1
		}
		sets = append(sets, "is_admin=?")
		args = append(args, v)
	}
	if p.IsDisabled != nil {
		v := 0
		if *p.IsDisabled {
			v = 1
		}
		sets = append(sets, "is_disabled=?")
		args = append(args, v)
	}
	if p.MaxActiveSessions != nil {
		sets = append(sets, "max_active_sessions=?")
		args = append(args, *p.MaxActiveSessions)
	}
	if p.EnableAllFolders != nil {
		v := 0
		if *p.EnableAllFolders {
			v = 1
		}
		sets = append(sets, "enable_all_folders=?")
		args = append(args, v)
	}
	if p.EnabledFolders != nil {
		b, _ := json.Marshal(*p.EnabledFolders)
		if len(*p.EnabledFolders) == 0 {
			b = []byte("[]")
		}
		sets = append(sets, "enabled_folders=?")
		args = append(args, string(b))
	}
	if p.BlockedTags != nil {
		b, _ := json.Marshal(*p.BlockedTags)
		if len(*p.BlockedTags) == 0 {
			b = []byte("[]")
		}
		sets = append(sets, "blocked_tags=?")
		args = append(args, string(b))
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	_, err := d.sql.ExecContext(ctx, "UPDATE users SET "+strings.Join(sets, ",")+" WHERE id=?", args...)
	return err
}
