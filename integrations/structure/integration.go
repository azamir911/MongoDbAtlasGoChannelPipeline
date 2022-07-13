package structure

import (
	"MongoDbAtlasGoChannelPipeline/integrations/elastic"
	"MongoDbAtlasGoChannelPipeline/integrations/mongo"
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"sync"
)

const (
	CyLogger = "cylogger"
)

type integrationRunner func(ctx context.Context, wg *sync.WaitGroup) <-chan *assetdata_model.NormalizedAsset

//type Integration interface {
//	DoExecute(ctx context.Context, wg *sync.WaitGroup) <-chan *assetdata_model.NormalizedAsset
//}

var integrations map[int]integrationRunner

func init() {
	integrations = make(map[int]integrationRunner)
	Register(57, elastic.DoExecute)
	Register(60, mongo.DoExecute)
}

func Register(integrationNumber int, theFunc integrationRunner) {
	integrations[integrationNumber] = theFunc
}

func Get(integrationNumber int) integrationRunner {
	return integrations[integrationNumber]
}
