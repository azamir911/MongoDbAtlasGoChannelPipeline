package assetdata_model

type Group struct {
	AssetDataBaseFields `mapstructure:",squash"`
	InheritsFromIds     []string        `json:"inheritsFromIds"`
	Permissions         []Permission    `json:"permissions"`
	UserIds             []string        `json:"userIds"`
	RoleIds             []RBACScopePair `json:"roleIds"`
	PolicyIds           []string        `json:"policyIds"`
}

func (Group) IsAssetData() {}
func (Group) IsRBACAsset() {}
