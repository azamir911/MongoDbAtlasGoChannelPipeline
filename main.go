package main

import (
	"MongoDbAtlasGoChannelPipeline/integrations/infrastructure"
	"MongoDbAtlasGoChannelPipeline/integrations/structure"
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"
)

const (
	CyLogger = "cylogger"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	log := zerolog.New(output).With().Timestamp().Logger()
	ctx, _ := context.WithCancel(context.WithValue(context.Background(), CyLogger, &log))

	var wg sync.WaitGroup
	mongoIntegrationRunner := structure.Get(60)
	elasticIntegrationRunner := structure.Get(57)

	mongoNormalizedAssetsCh := mongoIntegrationRunner(ctx, &wg)
	elasticNormalizedAssetsCh := elasticIntegrationRunner(ctx, &wg)
	//infrastructure.NormalizedAssetPrinter(
	//	ctx, &wg, normalizedGroupAssetFilter(
	//		ctx, &wg, mongoNormalizedAssetsCh,
	//	),
	//)

	//normalizedAssetsCh := make(chan *assetdata_model.NormalizedAsset, 10)
	normalizedAssetsCh := NormalizedAssetAggregator(
		ctx, &wg, mongoNormalizedAssetsCh, elasticNormalizedAssetsCh,
	)
	//infrastructure.NormalizedAssetAggregator(
	//	ctx, normalizedAssetsCh, &wg, mongoNormalizedAssetsCh,
	//)

	infrastructure.NormalizedAssetPrinter(
		ctx, &wg, normalizedAssetsCh,
	)

	wg.Wait()

	//time.Sleep(time.Second * 5)
	//cancelFunc()
}

func NormalizedAssetAggregator(ctx context.Context, wg *sync.WaitGroup, inputs ...<-chan *assetdata_model.NormalizedAsset) chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	var innerWg sync.WaitGroup
	go func() {
		defer func() {
			innerWg.Wait()
			log.Debug().Msg("Normalized Asset Aggregator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for i, in := range inputs {
			innerWg.Add(1)
			log.Debug().Msgf("Normalized Asset Aggregator %d processing working!", i+1)
			go func(output chan<- *assetdata_model.NormalizedAsset, index int, in <-chan *assetdata_model.NormalizedAsset) {
				defer innerWg.Done()
				log.Debug().Msgf("Normalized Asset Aggregator %d Inner Func processing working!", index)
				for x := range in {
					output <- x
				}
			}(output, i+1, in)
		}
	}()

	return output
}

func normalizedAssetFilterByType(ctx context.Context, wg *sync.WaitGroup, assetType assetdata_model.AssetType, input <-chan *assetdata_model.NormalizedAsset) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Normalized Asset Filter By Type Closing channel output!")
			close(output)
			wg.Done()
		}()

		for normalizedAsset := range input {
			log.Debug().Msg("Normalized Asset Filter By Type processing working!")
			//time.Sleep(time.Second)
			if normalizedAsset.Type == assetType {
				select {
				case output <- normalizedAsset:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}
