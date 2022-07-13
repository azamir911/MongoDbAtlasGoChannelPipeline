package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"io/ioutil"
	"net/http"

	"github.com/elastic/cloud-sdk-go/pkg/api"
	"github.com/elastic/cloud-sdk-go/pkg/api/deploymentapi"
	"github.com/elastic/cloud-sdk-go/pkg/api/deploymentapi/deputil"
	"github.com/elastic/cloud-sdk-go/pkg/client"
	"github.com/elastic/cloud-sdk-go/pkg/models"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type sdkApi struct {
	api *api.API
}

type restApi struct {
	apiKey     string
	httpClient *http.Client
}

type elasticCloudConnectorImpl struct {
	sdkApi  sdkApi
	restApi restApi
	clients []*elasticsearch.Client
}

// Verify that elasticCloudConnectorImpl satisfies ElasticCloudConnector.
var _ ElasticCloudConnector = (*elasticCloudConnectorImpl)(nil)

// GetAccount - https://www.elastic.co/guide/en/cloud/current/Accounts.html#get-current-account
func (e elasticCloudConnectorImpl) GetAccount(ctx context.Context) (response *AccountResponse, err error) {
	restApiSuffix := "account"
	responseBytes, err := e.doRestApi(ctx, restApiSuffix)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(responseBytes, &response)

	return response, err
}

// GetMembers - https://www.elastic.co/guide/en/cloud/current/Organizations.html#list-organization-members
func (e elasticCloudConnectorImpl) GetMembers(ctx context.Context, organizationId string) (response *OrganizationMemberships, err error) {
	restApiSuffix := fmt.Sprintf("organizations/%s/members", organizationId)
	responseBytes, err := e.doRestApi(ctx, restApiSuffix)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(responseBytes, &response)

	return response, err
}

// GetDeployments - returns the platform deployments
//DeploymentsListResponse https://www.elastic.co/guide/en/cloud/current/definitions.html#DeploymentsListResponse
func (e elasticCloudConnectorImpl) GetDeployments(_ context.Context) (*models.DeploymentsListResponse, error) {
	return deploymentapi.List(deploymentapi.ListParams{API: e.sdkApi.api})
}

func (e elasticCloudConnectorImpl) GetElasticSearch(_ context.Context, deployment *models.DeploymentsListingData, resource *models.DeploymentResource) (*models.ElasticsearchResourceInfo, error) {
	if deployment == nil {
		return nil, errors.New("got nil Elastic deployment")
	}
	if deployment.ID == nil {
		return nil, errors.New("got nil Elastic deployment id")
	}
	if resource == nil {
		return nil, errors.New("got nil Elastic deployment resource")
	}
	if resource.RefID == nil {
		return nil, errors.New("got nil Elastic deployment resource refId")
	}

	return deploymentapi.GetElasticsearch(deploymentapi.GetParams{
		API:                e.sdkApi.api,
		DeploymentID:       *deployment.ID,
		QueryParams:        deputil.QueryParams{},
		ConvertLegacyPlans: false,
		RefID:              *resource.RefID,
	})
}

func (e elasticCloudConnectorImpl) GetElasticSearchClientsSize(_ context.Context) int {
	return len(e.clients)
}

func (e elasticCloudConnectorImpl) GetElasticSearchUsers(ctx context.Context, clientIndex int) (response *ElasticSearchUsers, err error) {
	client := e.clients[clientIndex]
	contextFunc := client.Security.GetUser.WithContext(ctx)
	clientResponse, err := client.Security.GetUser(contextFunc)
	if err != nil {
		return nil, err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return nil, errors.New("got nil response from client when trying to get elastic search users")
	}
	defer clientResponse.Body.Close()
	err = e.parseResponse(ctx, clientResponse, &response)

	return response, err
}

func (e elasticCloudConnectorImpl) GetElasticSearchName(ctx context.Context, clientIndex int) (string, error) {

	client := e.clients[clientIndex]
	contextFunc := client.Cluster.GetSettings.WithContext(ctx)
	clientResponse, err := client.Cluster.GetSettings(contextFunc)

	if err != nil {
		return "", err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return "", errors.New("got nil response from client when trying to get elastic search cluster settings")
	}
	defer clientResponse.Body.Close()
	var response ElasticSearchMetaData
	err = e.parseResponse(ctx, clientResponse, &response)

	var displayName string
	if err == nil {
		displayName = response.Persistent.Cluster.Metadata.DisplayName
	}

	return displayName, err
}

func (e elasticCloudConnectorImpl) GetElasticSearchRoles(ctx context.Context, clientIndex int) (response *ElasticSearchRoles, err error) {

	client := e.clients[clientIndex]
	contextFunc := client.Security.GetRole.WithContext(ctx)
	clientResponse, err := client.Security.GetRole(contextFunc)
	if err != nil {
		return nil, err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return nil, errors.New("got nil response from client when trying to get roles")
	}
	defer clientResponse.Body.Close()
	err = e.parseResponse(ctx, clientResponse, &response)

	return response, err
}

func (e elasticCloudConnectorImpl) GetElasticSearchRepositories(ctx context.Context, clientIndex int) (repositories []string, err error) {

	client := e.clients[clientIndex]
	contextFunc := client.Snapshot.GetRepository.WithContext(ctx)
	clientResponse, err := client.Snapshot.GetRepository(contextFunc)
	if err != nil {
		return nil, err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return nil, errors.New("got nil response from client when trying to get repositories")
	}
	defer clientResponse.Body.Close()
	var response map[string]struct{}
	err = e.parseResponse(ctx, clientResponse, &response)

	for name := range response {
		repositories = append(repositories, name)
	}

	return repositories, err
}

func (e elasticCloudConnectorImpl) GetElasticSearchSnapshots(ctx context.Context, clientIndex int, repository string) (response *ElasticSearchSnapshots, err error) {

	client := e.clients[clientIndex]
	contextFunc := client.Snapshot.Get.WithContext(ctx)
	clientResponse, err := client.Snapshot.Get(repository, allSnapshots, contextFunc)
	if err != nil {
		return nil, err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return nil, errors.New("got nil response from client when trying to get snapshots")
	}
	defer clientResponse.Body.Close()
	err = e.parseResponse(ctx, clientResponse, &response)

	return response, err
}

func (e elasticCloudConnectorImpl) GetElasticSearchSnapshotsPolicies(ctx context.Context, clientIndex int) (response *ElasticSearchPolicies, err error) {

	client := e.clients[clientIndex]
	contextFunc := client.SlmGetLifecycle.WithContext(ctx)
	clientResponse, err := client.SlmGetLifecycle(contextFunc)
	if err != nil {
		return nil, err
	}
	if clientResponse == nil || clientResponse.Body == nil {
		return nil, errors.New("got nil response from client when trying to get snapshots polices")
	}
	defer clientResponse.Body.Close()
	err = e.parseResponse(ctx, clientResponse, &response)

	return response, err
}

func (e elasticCloudConnectorImpl) doRestApi(ctx context.Context, restApiSuffix string) ([]byte, error) {
	url := fmt.Sprintf("%s%s%s", api.ESSEndpoint, client.DefaultBasePath, restApiSuffix)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if request == nil {
		return nil, errors.New("got nil request from client")
	}
	request.Header.Add("Authorization", fmt.Sprintf("APIKey %s", e.restApi.apiKey))
	response, err := e.restApi.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("got nil response from rest client")
	}
	defer response.Body.Close()

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return responseBytes, nil
}

func (e elasticCloudConnectorImpl) parseResponse(ctx context.Context, clientResponse *esapi.Response, obj interface{}) error {
	responseBytes, err := ioutil.ReadAll(clientResponse.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(responseBytes, &obj)
	if err != nil {
		return err
	}

	return nil
}
