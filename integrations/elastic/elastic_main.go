package elastic

import (
	"MongoDbAtlasGoChannelPipeline/integrations/infrastructure"
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"sync"
)

func execute(ctx context.Context, outerWg *sync.WaitGroup, normalizedAssetsCh chan<- *assetdata_model.NormalizedAsset) {
	var wg sync.WaitGroup

	defer func() {
		close(normalizedAssetsCh)
		outerWg.Done()
	}()

	elasticCloudConnector := connector()

	normalizedUserAssetsChA := normalizedUserAssetCreator(
		ctx, &wg, normalizedElasticCloudUserCreator(
			ctx, &wg, elasticCloudUserPrinter(
				ctx, &wg, elasticCloudUserFilter(
					ctx, &wg, elasticCloudUsersMapper(
						ctx, &wg, elasticCloudUsersStreamer(
							ctx, &wg, elasticCloudConnector, accountStreamer(
								ctx, &wg, elasticCloudConnector,
							),
						),
					),
				),
			),
		),
	)

	infrastructure.NormalizedAssetAggregator(
		ctx, normalizedAssetsCh, &wg, normalizedUserAssetsChA,
	)

	wg.Wait()
}

func DoExecute(ctx context.Context, wg *sync.WaitGroup) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	normalizedAssetsCh := make(chan *assetdata_model.NormalizedAsset, 10)
	go execute(ctx, wg, normalizedAssetsCh)

	return normalizedAssetsCh
}
