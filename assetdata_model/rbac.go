package assetdata_model

type RoleType string

const AccessAll = "All"

type RoleEntity struct {
	Role       Role    `json:"role"`
	OnResource *string ` json:"resource"`
}

type RBACScope struct {
	Type   *AssetType `json:"type"`
	Entity string     `json:"entity"`
}

type Permission struct {
	Action string    `json:"action"`
	Scope  RBACScope `json:"scope"`
}

const (
	RoleTypeDefault RoleType = "Default"
	RoleTypeCustom  RoleType = "Custom"
)

type RBACScopePair struct {
	Id    string     `json:"id"`
	Scope *RBACScope `json:"scope"`
}

type RBACResourcePair struct {
	Name       string  `json:"name"`
	OnResource *string `json:"onResource"`
}

type Policy struct {
	AssetDataBaseFields `mapstructure:",squash"`
	Permissions         []Permission `json:"permissions"`
}

func (Policy) IsAssetData() {}

type Role struct {
	AssetDataBaseFields `mapstructure:",squash"`
	Type                *RoleType       `json:"type"`
	InheritsFromIds     []RBACScopePair `json:"inheritsFromIds"`
	Permissions         []Permission    `json:"permissions"`
	PolicyIds           []string        `json:"policyIds"`
}

func (Role) IsAssetData() {}
