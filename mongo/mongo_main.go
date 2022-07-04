package mongo

import (
	"context"
	"sync"
)

func Execute(ctx context.Context) {
	var wg sync.WaitGroup

	client := Client()
	//																									/ organizationPrinter
	// organizationsStreamer -> organizationsFilter -> organizationsMapper -> organizationsDuplicator -|-> atlasUsersStreamer
	// 																									\ teamsStreamer
	organizationsChA, organizationsCnB, organizationsCnC := organizationsDuplicator(
		ctx, &wg, organizationsMapper(
			ctx, &wg, organizationsFilter(
				ctx, &wg, organizationsStreamer(
					ctx, &wg, client,
				),
			),
		),
	)
	organizationPrinter(ctx, &wg, organizationsChA)

	// atlasUsersStreamer -> atlasUsersResponseMapper -> atlasUsersFilter -> atlasUserPrinter
	atlasUserPrinter(
		ctx, &wg, atlasUsersFilter(
			ctx, &wg, atlasUsersResponseMapper(
				ctx, &wg, atlasUsersStreamer(
					ctx, &wg, client, organizationsCnB,
				),
			),
		),
	)

	// teamsStreamer -> teamsMapper -> teamFilter -> teamPrinter
	teamPrinter(
		ctx, &wg, teamFilter(
			ctx, &wg, teamsMapper(
				ctx, &wg, teamsStreamer(
					ctx, &wg, client, organizationsCnC,
				),
			),
		),
	)

	// 																							    / projectPrinter
	// projectsStreamer -> projectsFilter -> projectsMapper -> projectFilter -> projectDuplicator -|-> clustersWithTeamsStreamer
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

	// databaseUsersStreamer -> databaseUsersMapper -> databaseUserFilter -> databaseUserPrinter
	databaseUserPrinter(
		ctx, &wg, databaseUserFilter(
			ctx, &wg, databaseUsersMapper(
				ctx, &wg, databaseUsersStreamer(
					ctx, &wg, client, projectsCnC,
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

	wg.Wait()
}
