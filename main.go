package main

import (
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

type merger func(ctx context.Context, wg *sync.WaitGroup, input1 <-chan *assetdata_model.NormalizedAsset, input2 <-chan *assetdata_model.NormalizedAsset)

type dependencies struct {
	sourceIntegrationId int
	targetIntegrationId int
	assetType           string
	theMerger           merger
}

//type dependency struct {
//}

func main() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
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

	normalizedAssets1 := normalizedAssetFilterByType(
		ctx, &wg, assetdata_model.UserAssetType, mongoNormalizedAssetsCh,
	)

	normalizedAssets2 := normalizedAssetFilterByType(
		ctx, &wg, assetdata_model.UserAssetType, elasticNormalizedAssetsCh,
	)

	// Doing crunching and print it
	normalizedUserAssetMerger3(
		ctx, &wg, normalizedAssets1, normalizedAssets2,
	)

	//normalizedAssetsCh := NormalizedAssetAggregator(
	//	ctx, &wg, normalizedAssets1, normalizedAssets2,
	//)

	//infrastructure.NormalizedAssetPrinter(
	//	ctx, &wg, normalizedAssetsCh,
	//)

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

func normalizedUserAssetMerger1(ctx context.Context, wg *sync.WaitGroup, inputs ...<-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)

	var innerWg sync.WaitGroup
	go func() {
		userMap := make(map[string]*assetdata_model.NormalizedAsset)
		defer func() {
			innerWg.Wait()
			log.Debug().Msg("Normalized User Asset Merger Closing!")

			for _, normalizedAsset := range userMap {
				log.Info().Msgf("\t\t\tDid not find crunching for Normalized Asset: %+v", normalizedAsset.Data)
			}

			for email := range userMap {
				delete(userMap, email)
			}

			wg.Done()
		}()

		for i, in := range inputs {
			innerWg.Add(1)
			log.Debug().Msgf("Normalized User Asset Merger %d processing working!", i+1)
			go func(input <-chan *assetdata_model.NormalizedAsset, i int) {
				defer func() {
					log.Debug().Msgf("Normalized User Asset Merger %d Closing!", i+1)
					innerWg.Done()
				}()

				for normalizedAsset := range input {
					if normalizedAsset != nil {
						user, ok := normalizedAsset.Data.(*assetdata_model.User)
						if ok {
							if len(user.Emails) > 0 {
								mapAsset, ok := userMap[user.Emails[0]]
								if ok {
									log.Debug().Msg("Normalized User Asset Merger processing working!")
									log.Info().Msgf("\t\t\tFind crunching for Normalized Asset: %+v %+v", normalizedAsset.Data, mapAsset.Data)
									delete(userMap, user.Emails[0])
								} else {
									userMap[user.Emails[0]] = normalizedAsset
								}
							}
						}
					}
					//}
				}
			}(in, i)
		}
	}()
}

func normalizedUserAssetMerger2(ctx context.Context, wg *sync.WaitGroup, input1 <-chan *assetdata_model.NormalizedAsset, input2 <-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)

	var innerWg sync.WaitGroup
	go func() {
		userMap := make(map[string]*assetdata_model.NormalizedAsset)
		defer func() {
			innerWg.Wait()
			log.Debug().Msg("Normalized User Asset Merger Closing!")

			for _, normalizedAsset := range userMap {
				log.Info().Msgf("\t\t\tDid not find crunching for Normalized Asset: %+v", normalizedAsset.Data)
			}

			for email := range userMap {
				delete(userMap, email)
			}

			wg.Done()
		}()

		in := input1
		i := 1
		innerWg.Add(1)
		log.Debug().Msgf("Normalized User Asset Merger %d processing working!", i)
		go func(input <-chan *assetdata_model.NormalizedAsset, i int) {
			defer func() {
				log.Debug().Msgf("Normalized User Asset Merger %d Closing!", i)
				innerWg.Done()
			}()

			for normalizedAsset := range input {
				if normalizedAsset != nil {
					user, ok := normalizedAsset.Data.(*assetdata_model.User)
					if ok {
						if len(user.Emails) > 0 {
							mapAsset, ok := userMap[user.Emails[0]]
							if ok {
								log.Debug().Msg("Normalized User Asset Merger processing working!")
								log.Info().Msgf("\t\t\tFind crunching for Normalized Asset: %+v %+v", normalizedAsset.Data, mapAsset.Data)
								delete(userMap, user.Emails[0])
							} else {
								userMap[user.Emails[0]] = normalizedAsset
							}
						}
					}
				}
			}
		}(in, i)

		in = input2
		i = 2
		innerWg.Add(1)
		log.Debug().Msgf("Normalized User Asset Merger %d processing working!", i)
		go func(input <-chan *assetdata_model.NormalizedAsset, i int) {
			defer func() {
				log.Debug().Msgf("Normalized User Asset Merger %d Closing!", i)
				innerWg.Done()
			}()

			for normalizedAsset := range input {
				if normalizedAsset != nil {
					user, ok := normalizedAsset.Data.(*assetdata_model.User)
					if ok {
						if len(user.Emails) > 0 {
							mapAsset, ok := userMap[user.Emails[0]]
							if ok {
								log.Debug().Msg("Normalized User Asset Merger processing working!")
								log.Info().Msgf("\t\t\tFind crunching for Normalized Asset: %+v %+v", normalizedAsset.Data, mapAsset.Data)
								delete(userMap, user.Emails[0])
							} else {
								userMap[user.Emails[0]] = normalizedAsset
							}
						}
					}
				}
			}
		}(in, i)
	}()
}

func normalizedUserAssetMerger3(ctx context.Context, wg *sync.WaitGroup, input1 <-chan *assetdata_model.NormalizedAsset, input2 <-chan *assetdata_model.NormalizedAsset) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)

	go func() {
		userMap := make(map[string]*assetdata_model.NormalizedAsset)
		defer func() {
			log.Debug().Msg("Normalized User Asset Merger Closing!")

			for _, normalizedAsset := range userMap {
				log.Info().Msgf("\t\t\tDid not find crunching for Normalized Asset: %+v", normalizedAsset.Data)
			}

			for email := range userMap {
				delete(userMap, email)
			}

			wg.Done()
		}()

		for {
			select {
			case normalizedAsset1 := <-input1:
				if normalizedAsset1 != nil {
					user, ok := normalizedAsset1.Data.(*assetdata_model.User)
					if ok {
						if len(user.Emails) > 0 {
							mapAsset, ok := userMap[user.Emails[0]]
							if ok {
								log.Debug().Msg("Normalized Asset Merger processing working!")
								log.Info().Msgf("\t\t\tFind crunching for Normalized Asset: %+v %+v", normalizedAsset1.Data, mapAsset.Data)
								delete(userMap, user.Emails[0])
							} else {
								userMap[user.Emails[0]] = normalizedAsset1
							}
						}
					}
				} else {
					log.Debug().Msgf("Normalized User Asset Merger %d Closing!", 1)
					input1 = nil
				}
			case normalizedAsset2 := <-input2:
				if normalizedAsset2 != nil {
					user, ok := normalizedAsset2.Data.(*assetdata_model.User)
					if ok {
						if len(user.Emails) > 0 {
							if len(user.Emails) > 0 {
								mapAsset, ok := userMap[user.Emails[0]]
								if ok {
									log.Debug().Msg("Normalized Asset Merger processing working!")
									log.Info().Msgf("\t\t\tFind crunching for Normalized Asset: %+v %+v", normalizedAsset2.Data, mapAsset.Data)
									delete(userMap, user.Emails[0])
								} else {
									userMap[user.Emails[0]] = normalizedAsset2
								}
							}
						}
					}
				} else {
					log.Debug().Msgf("Normalized User Asset Merger %d Closing!", 2)
					input2 = nil
				}
			}

			if input1 == nil && input2 == nil {
				break
			}
		}
	}()
}
