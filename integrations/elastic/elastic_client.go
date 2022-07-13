package elastic

import (
	connector2 "MongoDbAtlasGoChannelPipeline/integrations/elastic/connector"
	"net/http"
)

const (
	CyLogger = "cylogger"
)

func connector() connector2.ElasticCloudConnector {
	client := new(http.Client)
	apiKey := "Q0dMZUZJRUJoVTN3V2d3ZWpvQkQ6SWZQVVdWUm5SWGFjRHRsOTBHbDVqUQ=="
	var deployments map[string]string = make(map[string]string)
	cloudId := "CypagoDeploymentNew2:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQxOTczN2RiYWRmNWQ0YmM4YjRmOTdiN2YwZDNhYzM3YyQ5N2M0MzIyOGFlMzk0MTU0YThhMjZlYWMyMzVjNjg4OQ=="
	deploymentApiKey := "YXo1aEg0RUJEYm11QVBPenRlZzA6YkNIMWlBNHBSbnVyUHlhMU1vRXV2Zw=="
	deployments[cloudId] = deploymentApiKey

	elasticCloudConnector, _ := connector2.NewElasticCloudConnector(client, apiKey, deployments)

	return elasticCloudConnector
}
