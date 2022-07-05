package assetdata_model

import (
	"github.com/google/uuid"
)

type NormalizedAsset struct {
	Id            uuid.UUID
	AccountId     string
	IntegrationId int
	Type          AssetType
	Data          AssetData
}
