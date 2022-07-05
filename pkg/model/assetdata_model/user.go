package assetdata_model

import "time"

type UserStatus string

const (
	UserStatusActive   UserStatus = "Active"
	UserStatusInactive UserStatus = "Inactive"
	UserStatusDisabled UserStatus = "Disabled"
)

type User struct {
	AssetDataBaseFields    `mapstructure:",squash"`
	UserName               *string         `json:"userName"`
	Emails                 []string        `json:"emails"`
	CreatedAt              *time.Time      `json:"createdAt"`
	MfaEnabled             *bool           `json:"mfaEnabled"`
	SsoEnabled             *bool           `json:"ssoEnabled"`
	SsoAppIds              []string        `json:"ssoAppIds"`
	SsoProviderIntegration *int            `json:"ssoProviderIntegration"`
	LastLogin              *time.Time      `json:"lastLogin"`
	IsAdmin                *bool           `json:"isAdmin"`
	Status                 *UserStatus     `json:"status"`
	GroupIds               []string        `json:"groupIds"`
	RoleIds                []RBACScopePair `json:"roleIds"`
	PolicyIds              []string        `json:"policyIds"`
	Permissions            []Permission    `json:"permissions"`
}

func (User) IsAssetData() {}
func (User) IsRBACAsset() {}
