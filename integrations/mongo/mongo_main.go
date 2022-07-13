package mongo

import (
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"github.com/rs/zerolog"
	"sync"
)

func execute(ctx context.Context, outerWg *sync.WaitGroup, normalizedAssetsCh chan<- *assetdata_model.NormalizedAsset) {
	var wg sync.WaitGroup

	defer func() {
		close(normalizedAssetsCh)
		outerWg.Done()
	}()

	client := Client()
	//																									/ organizationPrinter
	// organizationsStreamer -> organizationsFilter -> organizationsMapper -> organizationDuplicator -|-> atlasUsersStreamer
	// 																									\ teamsStreamer
	organizationsChA, organizationsCnB, organizationsCnC := organizationDuplicator(
		ctx, &wg, organizationsMapper(
			ctx, &wg, organizationsFilter(
				ctx, &wg, organizationsStreamer(
					ctx, &wg, client,
				),
			),
		),
	)
	organizationPrinter(ctx, &wg, organizationsChA)

	// atlasUsersStreamer -> atlasUsersResponseMapper -> atlasUserFilter -> atlasUserPrinter -> normalizedAtlasUserCreator -> normalizedUserAssetCreator -> normalizedAssetAggregator
	normalizedUserAssetsChA := normalizedUserAssetCreator(
		ctx, &wg, normalizedAtlasUserCreator(
			ctx, &wg, atlasUserPrinter(
				ctx, &wg, atlasUserFilter(
					ctx, &wg, atlasUsersResponseMapper(
						ctx, &wg, atlasUsersStreamer(
							ctx, &wg, client, organizationsCnB,
						),
					),
				),
			),
		),
	)

	// teamsStreamer -> teamsMapper -> teamFilter -> teamPrinter -> normalizedAtlasTeamCreator -> normalizedGroupAssetCreator -> normalizedAssetAggregator
	normalizedGroupAssetsCh := normalizedGroupAssetCreator(
		ctx, &wg, normalizedAtlasTeamCreator(
			ctx, &wg, teamPrinter(
				ctx, &wg, teamFilter(
					ctx, &wg, teamsMapper(
						ctx, &wg, teamsStreamer(
							ctx, &wg, client, organizationsCnC,
						),
					),
				),
			),
		),
	)

	// 																							    / projectPrinter
	// projectsStreamer -> projectsFilter -> projectsMapper -> projectFilter -> projectDuplicator -|-> teamsAssignedStreamer
	//																							   |\ databaseUsersStreamer
	//																							    \ customDbRolesStreamer
	projectsCnA, projectsCnB, projectsCnC, projectsCnD := projectDuplicator(
		ctx, &wg, projectFilter(
			ctx, &wg, projectsMapper(
				ctx, &wg, projectsFilter(
					ctx, &wg, projectsStreamer(
						ctx, &wg, client,
					),
				),
			),
		),
	)
	projectPrinter(ctx, &wg, projectsCnA)

	// 																					   							/ clusterWithTeamsPrinter
	// teamsAssignedStreamer -> clustersWithTeamsStreamer -> clustersWithTeamsMapper -> clusterWithTeamsDuplicator |- clusterWithTeamsMapper
	clusterWithTeamsCnA, clusterWithTeamsCnB := clusterWithTeamsDuplicator(
		ctx, &wg, clustersWithTeamsMapper(
			ctx, &wg, clustersWithTeamsStreamer(
				ctx, &wg, client, teamsAssignedStreamer(
					ctx, &wg, client, projectsCnB,
				),
			),
		),
	)
	clusterWithTeamsPrinter(ctx, &wg, clusterWithTeamsCnA)

	// 											    / snapshotsStreamer
	// clusterWithTeamsMapper -> clusterDuplicator |- snapshotsStreamer
	//											    \ snapshotsRestoreJobsStreamer

	clusterCnA, clusterCnB, clusterCnC := clusterDuplicator(
		ctx, &wg, clusterWithTeamsMapper(
			ctx, &wg, clusterWithTeamsCnB,
		),
	)

	// databaseUsersStreamer -> databaseUsersMapper -> databaseUserFilter -> databaseUserPrinter -> normalizedDatabaseUserCreator -> normalizedUserAssetCreator -> normalizedAssetAggregator
	normalizedUserAssetsChB := normalizedUserAssetCreator(
		ctx, &wg, normalizedDatabaseUserCreator(
			ctx, &wg, databaseUserPrinter(
				ctx, &wg, databaseUserFilter(
					ctx, &wg, databaseUsersMapper(
						ctx, &wg, databaseUsersStreamer(
							ctx, &wg, client, projectsCnC,
						),
					),
				),
			),
		),
	)

	// customDbRolesStreamer -> customDbRolesMapper -> customDbRoleFilter -> customDbRolePrinter
	customDbRolePrinter(
		ctx, &wg, customDbRoleFilter(
			ctx, &wg, customDbRolesMapper(
				ctx, &wg, customDbRolesStreamer(
					ctx, &wg, client, projectsCnD,
				),
			),
		),
	)

	// SnapshotsStreamer1 \
	//						| -> snapshotsAggregator -> snapshotsMapper -> snapshotFilter -> snapshotPrinter
	// SnapshotsStreamer2 /
	streamer1 := snapshotsStreamer(
		ctx, &wg, client, clusterCnA, 1,
	)
	streamer2 := snapshotsStreamer(
		ctx, &wg, client, clusterCnB, 2,
	)
	snapshotPrinter(
		ctx, &wg, snapshotFilter(
			ctx, &wg, snapshotsMapper(
				ctx, &wg, snapshotsAggregator(
					ctx, &wg, streamer1, streamer2,
				),
			),
		),
	)

	// snapshotsRestoreJobsStreamer -> snapshotsRestoreJobsMapper -> snapshotRestoreJobFilter -> snapshotRestoreJobPrinter
	snapshotRestoreJobPrinter(
		ctx, &wg, snapshotRestoreJobFilter(
			ctx, &wg, snapshotsRestoreJobsMapper(
				ctx, &wg, snapshotsRestoreJobsStreamer(
					ctx, &wg, client, clusterCnC,
				),
			),
		),
	)

	// normalizedUserAssetCreator (atlas)  	 \
	// normalizedUserAssetCreator (database) | -> normalizedAssetAggregator
	// normalizedGroupAssetCreator			 /
	normalizedAssetAggregator(
		ctx, normalizedAssetsCh, &wg, normalizedUserAssetsChA, normalizedUserAssetsChB, normalizedGroupAssetsCh,
	)

	wg.Wait()
}

func DoExecute(ctx context.Context, wg *sync.WaitGroup) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	normalizedAssetsCh := make(chan *assetdata_model.NormalizedAsset, 10)
	go execute(ctx, wg, normalizedAssetsCh)

	return normalizedAssetsCh
}

func normalizedAssetAggregator(ctx context.Context, output chan<- *assetdata_model.NormalizedAsset, wg *sync.WaitGroup, inputs ...<-chan *assetdata_model.NormalizedAsset) {
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

func normalizedAssetCleaner(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.NormalizedAsset) {
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
