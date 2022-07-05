package assetdata_model

type AssetDataBaseFields struct {
	ID          string  `json:"id"`
	Name        *string `json:"name"`
	Integration int     `json:"integration"`
	Account     string  `json:"account"`
}
