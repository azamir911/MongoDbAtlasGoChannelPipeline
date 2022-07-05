package assetdata_model

type AssetType string

const (
	// Cloud providers
	FileAssetType           AssetType = "File"
	DocumentAssetType       AssetType = "Document"
	PolicyDocumentAssetType AssetType = "PolicyDocument"
	LinkAssetType           AssetType = "Link"

	StorageAssetType                 AssetType = "Storage"
	UserAssetType                    AssetType = "User"
	BucketAssetType                  AssetType = "Bucket"
	ServerDiskAssetType              AssetType = "ServerDisk"
	AccountAssetType                 AssetType = "Account"
	SecretAssetType                  AssetType = "Secret"
	FirewallAssetType                AssetType = "Firewall"
	DatabaseAssetType                AssetType = "Database"
	BackupAssetType                  AssetType = "Backup"
	BackupConfigurationAssetType     AssetType = "BackupConfiguration"
	BackupRestorationResultAssetType AssetType = "BackupRestorationResult"
	AuditTrailLogAssetType           AssetType = "AuditTrailLog"
	ApplicationLogAssetType          AssetType = "ApplicationLog"

	// Change management
	ChangeManagementTicketAssetType AssetType = "ChangeManagementTicket"
	// Source control
	PullRequestAssetType       AssetType = "PullRequest"
	PullRequestReviewAssetType AssetType = "PullRequestReview"
	BranchAssetType            AssetType = "Branch"
	RepositoryAssetType        AssetType = "Repository"
	// CRM
	SupportTicketAssetType         AssetType = "SupportTicket"
	CustomerSupportTicketAssetType AssetType = "CustomerSupportTicket"

	// Company
	EmployeeAssetType AssetType = "Employee"

	PolicyAssetType  AssetType = "Policy"
	GroupAssetType   AssetType = "Group"
	RoleAssetType    AssetType = "Role"
	DeviceAssetType  AssetType = "Device"
	ProjectAssetType AssetType = "Project"

	//Security
	VulnerabilityScanReportAssetType        AssetType = "VulnerabilityScanReport"
	VulnerabilityScanConfigurationAssetType AssetType = "VulnerabilityScanConfiguration"

	// General
	AlertEventAssetType             AssetType = "AlertEvent"
	AlertRuleConfigurationAssetType AssetType = "AlertRuleConfiguration"
	IncidentAssetType               AssetType = "Incident"
	AppAssetType                    AssetType = "App"

	// CI CD
	JobConfigurationAssetType AssetType = "JobConfiguration"
	BuildAssetType            AssetType = "Build"

	// Monitor
	MonitoringDashboardType AssetType = "MonitoringDashboard"
)
