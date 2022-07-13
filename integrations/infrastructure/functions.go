package infrastructure

import (
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"github.com/rs/zerolog"
	"sync"
)

const (
	CyLogger = "cylogger"
)

func NormalizedAssetAggregator(ctx context.Context, output chan<- *assetdata_model.NormalizedAsset, wg *sync.WaitGroup, inputs ...<-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	var innerWg sync.WaitGroup
	go func(output chan<- *assetdata_model.NormalizedAsset) {
		defer func() {
			innerWg.Wait()
			log.Debug().Msg("Normalized Asset Aggregator Closing channel output!")
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
	}(output)
}

func NormalizedAssetCleaner(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Empty Normalized Asset Printer exit")
			wg.Done()
		}()

		for normalizedAsset := range input {
			log.Debug().Msg("Empty Normalized Asset Printer processing working!")
			log.Debug().Msgf("\tEmpty Normalized Asset: %+v %+v", normalizedAsset, normalizedAsset.Data)
		}
	}()
}

func NormalizedAssetPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)

	go func() {
		defer wg.Done()

		for normalizedAsset := range input {
			log.Debug().Msg("Normalized Asset Printer processing working!")
			log.Info().Msgf("\tNormalized Asset: %+v %+v", normalizedAsset, normalizedAsset.Data)
		}
	}()
}
