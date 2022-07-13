package connector

import (
	"context"
	"net/http"
	"time"

	"github.com/elastic/cloud-sdk-go/pkg/api"
	"github.com/elastic/cloud-sdk-go/pkg/auth"
	"github.com/elastic/cloud-sdk-go/pkg/models"
	"github.com/elastic/go-elasticsearch/v8"
)

var allSnapshots = []string{"*"}

type ElasticCloudConnector interface {
	GetAccount(ctx context.Context) (*AccountResponse, error)
	GetMembers(ctx context.Context, organizationId string) (*OrganizationMemberships, error)
	GetDeployments(ctx context.Context) (*models.DeploymentsListResponse, error)
	GetElasticSearch(ctx context.Context, deployment *models.DeploymentsListingData, resource *models.DeploymentResource) (*models.ElasticsearchResourceInfo, error)
	GetElasticSearchClientsSize(ctx context.Context) int
	GetElasticSearchUsers(ctx context.Context, clientIndex int) (*ElasticSearchUsers, error)
	GetElasticSearchName(ctx context.Context, clientIndex int) (string, error)
	GetElasticSearchRoles(ctx context.Context, clientIndex int) (*ElasticSearchRoles, error)
	GetElasticSearchRepositories(ctx context.Context, clientIndex int) ([]string, error)
	GetElasticSearchSnapshots(ctx context.Context, clientIndex int, repository string) (*ElasticSearchSnapshots, error)
	GetElasticSearchSnapshotsPolicies(ctx context.Context, clientIndex int) (*ElasticSearchPolicies, error)
}

// AccountResponse https://www.elastic.co/guide/en/cloud/current/definitions.html#AccountResponse
type AccountResponse struct {
	Id string `json:"id"`
}

// OrganizationMembership https://www.elastic.co/guide/en/cloud/current/definitions.html#OrganizationMembership
type OrganizationMembership struct {
	OrganizationId string `json:"organization_id"`
	UserId         string `json:"user_id"`
	Name           string `json:"name"`
	Email          string `json:"email"`
	MemberSince    string `json:"member_since"`
}

// OrganizationMemberships https://www.elastic.co/guide/en/cloud/current/definitions.html#OrganizationMemberships
type OrganizationMemberships struct {
	Members []OrganizationMembership `json:"members"`
}

// ElasticSearchUsers https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-user.html
type ElasticSearchUsers map[string]ElasticSearchUser

// ElasticSearchUser https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-user.html
type ElasticSearchUser struct {
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	FullName string   `json:"full_name"`
	Email    string   `json:"email"`
	Enabled  bool     `json:"enabled"`
}

type ElasticSearchMetaData struct {
	Persistent struct {
		Cluster struct {
			Metadata struct {
				DisplayName string `json:"display_name"`
			} `json:"metadata"`
		} `json:"cluster"`
	}
}

// ElasticSearchRoles https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role.html
type ElasticSearchRoles map[string]ElasticSearchRole

// ElasticSearchRole https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role.html
type ElasticSearchRole struct {
	Cluster  []string `json:"cluster"`
	Metadata struct {
		Deprecated bool `json:"_deprecated"`
		Reserved   bool `json:"_reserved"`
	} `json:"metadata"`
}

// ElasticSearchSnapshots https://www.elastic.co/guide/en/elasticsearch/reference/current/get-snapshot-api.html
type ElasticSearchSnapshots struct {
	Snapshots []ElasticSearchSnapshot `json:"snapshots"`
	Total     int                     `json:"total"`
	Remaining int                     `json:"remaining"`
}

// ElasticSearchSnapshot https://www.elastic.co/guide/en/elasticsearch/reference/current/get-snapshot-api.html
type ElasticSearchSnapshot struct {
	Name       string `json:"snapshot"`   // Name of the snapshot.
	Id         string `json:"uuid"`       // Unique identifier of the snapshot.
	Repository string `json:"repository"` // Name of the repository of the snapshot.
	VersionID  int    `json:"version_id"`
	Version    string `json:"version"`
	Metadata   struct {
		Policy string `json:"policy"`
	} `json:"metadata,omitempty"`
	Status    string    `json:"state"`      // Current status of the snapshot. Possible values are: IN_PROGRESS, SUCCESS, FAILED and PARTIAL.
	StartTime time.Time `json:"start_time"` // Date of when the snapshot creation process started
	EndTime   time.Time `json:"end_time"`   // Date of when the snapshot creation process ended
}

// ElasticSearchPolicies https://www.elastic.co/guide/en/elasticsearch/reference/current/slm-api-get-policy.html
type ElasticSearchPolicies map[string]ElasticSearchPolicy

// ElasticSearchPolicy
// https://www.elastic.co/guide/en/elasticsearch/reference/current/slm-api-get-policy.html
// https://www.elastic.co/guide/en/elasticsearch/reference/current/slm-api-put-policy.html
type ElasticSearchPolicy struct {
	Version            int   `json:"version"`
	ModifiedDateMillis int64 `json:"modified_date_millis"`
	Policy             struct {
		Name       string `json:"name"`
		Schedule   string `json:"schedule"`
		Repository string `json:"repository"`
		Retention  struct {
			ExpireAfter string `json:"expire_after"`
			MinCount    int    `json:"min_count"`
			MaxCount    int    `json:"max_count"`
		} `json:"retention"`
	} `json:"policy"`
	LastSuccess struct {
		SnapshotName string `json:"snapshot_name"`
		StartTime    int64  `json:"start_time"`
		Time         int64  `json:"time"`
	} `json:"last_success"`
	LastFailure struct {
		SnapshotName string `json:"snapshot_name"`
		Time         int64  `json:"time"`
		Details      string `json:"details"`
	} `json:"last_failure"`
	NextExecutionMillis int64 `json:"next_execution_millis"`
	Stats               struct {
		Policy                   string `json:"policy"`
		SnapshotsTaken           int    `json:"snapshots_taken"`
		SnapshotsFailed          int    `json:"snapshots_failed"`
		SnapshotsDeleted         int    `json:"snapshots_deleted"`
		SnapshotDeletionFailures int    `json:"snapshot_deletion_failures"`
	} `json:"stats"`
}

type Deployment struct {
	CloudID string // Endpoint for the Elastic Service (https://elastic.co/cloud).
	APIKey  string // Base64-encoded token for authorization; if set, overrides username/password and service token.
}

func NewElasticCloudConnector(httpClient *http.Client, apiKey string, deployments map[string]string) (ElasticCloudConnector, error) {
	newAPI, err := api.NewAPI(api.Config{
		Client:     httpClient,
		AuthWriter: auth.APIKey(apiKey),
	})

	if err != nil {
		return nil, err
	}

	var elasticsearchClients []*elasticsearch.Client
	for key, value := range deployments {
		config := elasticsearch.Config{
			CloudID:   key,
			APIKey:    value,
			Transport: httpClient.Transport,
		}

		elasticsearchClient, err := elasticsearch.NewClient(config)
		if err != nil {
			return nil, err
		}
		elasticsearchClients = append(elasticsearchClients, elasticsearchClient)
	}

	c := &elasticCloudConnectorImpl{
		sdkApi: sdkApi{
			api: newAPI,
		},
		restApi: restApi{
			apiKey:     apiKey,
			httpClient: httpClient,
		},
		clients: elasticsearchClients,
	}

	return c, nil
}
