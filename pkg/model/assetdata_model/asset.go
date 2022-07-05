package assetdata_model

import (
	"time"

	"github.com/google/uuid"
)

type AssetDataLoaderInput struct {
	DataId        string
	Account       string
	Type          AssetType
	IntegrationId int
}

type AssetData interface {
	IsAssetData()
}

type assetBaseFields struct {
	ID              uuid.UUID
	TenantId        uuid.UUID     `db:"tenant_id"`
	ScanId          uuid.NullUUID `db:"scan_id"`
	Type            AssetType
	TimeCollected   time.Time  `db:"time_collected"`
	LastTimeUpdated *time.Time `db:"last_time_updated"`
	RawDataLinks    []string   `db:"raw_data_links"`
	IntegrationId   *int       `db:"integration_id"`
	Account         *string
	//Ignored         *IgnoreDetails // Will be populated only in snapshots
}

type Asset struct {
	assetBaseFields
	Data AssetData
}

// AssetMapData same as model.Asset, just with different type of Data field map[string]
type AssetMapData struct {
	assetBaseFields
	Data map[string]interface{}
}

type AssetMetadata struct {
	TenantId      uuid.UUID  `db:"tenant_id"`
	ID            uuid.UUID  `db:"asset_id"`
	AuditEeId     *uuid.UUID `db:"audits_ee_id"`
	UniversalEeId *string    `db:"universal_ee_id"`
	EntityId      uuid.UUID  `db:"entity_id"`
}

/*
type AssetInput struct {
	Name         *string
	Type         assetdata_model.AssetType
	AuditEeId    uuid.UUID
	FileUpload   *graphql.Upload
	RemoteUpload *RemoteUpload
	UrlUpload    *UrlUpload
}

type RemoteUpload struct {
	Name          *string
	FileName      string
	FilePath      string
	FileId        string
	DriveId       *string
	MimeType      *string
	Thumbnail     *string
	IntegrationId int
}

type AssetPageInfo struct {
	Type           assetdata_model.AssetType
	IntegrationIds []int
	TotalCount     *int
	Assets         []*Asset
	More           bool
}

type UrlUpload struct {
	Url  string
	Name *string
}

type AssetCount struct {
	Type  assetdata_model.AssetType
	Count int
}

type AssetRepo interface {
	// GetAssetsForAudit fetches all assets per a given auditId. scanId is optional, if not supplied, it will be populated by tenants.LastScanId
	GetAssetsForAudit(ctx context.Context, tenantId, auditId, scanId uuid.UUID) ([]*Asset, error)
	GetAssets(ctx context.Context, tenantId uuid.UUID, scanId *uuid.UUID, ids []uuid.UUID, types []assetdata_model.AssetType, integrationIds []common.IntegrationId, entityId *uuid.UUID, pagination *PaginationInput) ([]*Asset, error)
	GetAssetsByDataIdMap(ctx context.Context, tenantId uuid.UUID, scanId *uuid.UUID, types []assetdata_model.AssetType, integrationIds []common.IntegrationId, dataIds []string, entityId *uuid.UUID) (map[string][]*AssetMapData, error)
	GetAssetMap(ctx context.Context, tenantId uuid.UUID, ids []uuid.UUID) (map[uuid.UUID]*Asset, error)
	GetAssetsByAuditEeId(ctx context.Context, tenantId, scanId uuid.UUID, auditEeIds []uuid.UUID) (map[uuid.UUID][]*Asset, error)
	// GetAssetSummary returns a summary of all asset types according to filters.
	// If both frameworkTypes and auditIds are not supplied, returns a summary of all tenant's assets (filtered by entity if provided)
	GetAssetSummary(ctx context.Context, tenantId uuid.UUID, frameworkTypes []FrameworkType, auditIds []uuid.UUID, entityId *uuid.UUID) ([]*AssetCount, error)
	GetAssetsByAuditEeIdFirstPage(ctx context.Context, tenantId, auditEeId, snapshotId uuid.UUID, pageSize int) ([]*AssetPageInfo, error)
	GetAssetsByAuditEeIdAndType(ctx context.Context, tenantId, scanId, auditEeId, snapshotId uuid.UUID, assetType assetdata_model.AssetType, pagination *PaginationInput) (*AssetPageInfo, error)

	// GetUniEesWhichHaveAutoAssets returns a map of uniEes which have auto-collected assets
	GetUniEesWhichHaveAutoAssets(ctx context.Context, tenantId, scanId uuid.UUID, entityId uuid.UUID) (map[UniversalEeId]bool, error)

	// GetUniEesWhichHaveManualAssets returns a map of uniEes which have manual files
	GetUniEesWhichHaveManualAssets(ctx context.Context, tenantId, scanId uuid.UUID, auditId *uuid.UUID, types []assetdata_model.AssetType) (map[UniversalEeId]bool, error)

	StoreAsset(ctx context.Context, outerTx Tx, a *Asset, auditEeId *uuid.UUID) (*Asset, error)
	DeleteAssets(ctx context.Context, outerTx Tx, tenantId uuid.UUID, ids []uuid.UUID) ([]*Asset, []*AssetMetadata, error)

	// TakeEeSnapshot creates a new snapshot for auditsEeId.
	// If a previous snapshot exists for this auditEeId and overrideAnyExisting is false, common.ObjectAlreadyExistsError error will be returned
	TakeEeSnapshot(ctx context.Context, tx Tx, tenantId, auditId, auditEeId uuid.UUID, submitted, overrideAnyExisting bool) (EeSnapshot, error)

	// DeleteEeSnapshot deletes a snapshot. Returns common.NoContentError if not found
	DeleteEeSnapshot(ctx context.Context, outerTx Tx, tenantId, auditId, auditEeId, snapshotId uuid.UUID) (EeSnapshot, error)

	// SubmitEeSnapshot returns a common.BadRequestError if snapshot has already been submitted
	SubmitEeSnapshot(ctx context.Context, outerTx Tx, tenantId, auditId, auditEeId, snapshotId uuid.UUID) (EeSnapshot, error)
	GetEeSnapshotByAuditEeIdMap(ctx context.Context, outerTx Tx, tenantId uuid.UUID, auditEeIds []uuid.UUID) (map[uuid.UUID]*EeSnapshot, error)
}
*/
