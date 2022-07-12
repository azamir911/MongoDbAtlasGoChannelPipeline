package main

import (
	"MongoDbAtlasGoChannelPipeline/integrations/mongo"
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

	mongoNormalizedAssetsCh := mongo.DoExecute(ctx, &wg)
	normalizedAssetPrinter(
		ctx, &wg, normalizedGroupAssetFilter(
			ctx, &wg, mongoNormalizedAssetsCh,
		),
	)

	wg.Wait()

	//time.Sleep(time.Second * 5)
	//cancelFunc()
}

func normalizedAssetPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)

	go func() {
		defer wg.Done()

		for normalizedAsset := range input {
			log.Debug().Msg("Atlas Normalized Asset Printer processing working!")
			log.Info().Msgf("\tNormalized Asset: %+v %+v", normalizedAsset, normalizedAsset.Data)
		}
	}()
}

func normalizedGroupAssetFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.NormalizedAsset) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Normalized Group Asset Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for normalizedAsset := range input {
			log.Debug().Msg("Normalized Group Asset Filter processing working!")
			//time.Sleep(time.Second)
			if normalizedAsset.Type == assetdata_model.GroupAssetType {
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
