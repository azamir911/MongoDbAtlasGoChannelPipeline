package mongo

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func snapshotsStreamer(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AdvancedCluster, startPage int) <-chan *mongodbatlas.CloudProviderSnapshots {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshots, 10)

	go func() {
		defer func() {
			log.Debug().Msgf("Mongo: Snapshots Streamer %d Closing channel output!", startPage)
			close(output)
			wg.Done()
		}()

		client := newClient()

		for cluster := range input {
			snapshotReqPathParameters := &mongodbatlas.SnapshotReqPathParameters{
				GroupID:     cluster.GroupID,
				ClusterName: cluster.Name,
			}

			// Declare the option to get only one team id
			options := &mongodbatlas.ListOptions{
				//PageNum:      1,
				PageNum:      startPage,
				ItemsPerPage: 2,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				snapshots, _, err := client.CloudProviderSnapshots.GetAllCloudProviderSnapshots(ctx, snapshotReqPathParameters, options)
				if err != nil {
					log.Err(err).Msg("Mongo: Failed to get cloud provider snapshots list")
					break
				}
				if snapshots == nil || len(snapshots.Results) == 0 {
					break
				}

				select {
				case output <- snapshots:
				case <-ctx.Done():
					return
				}

				options.PageNum = options.PageNum + 2
			}
		}
	}()
	return output
}

func snapshotsAggregator(ctx context.Context, wg *sync.WaitGroup, inputs ...<-chan *mongodbatlas.CloudProviderSnapshots) <-chan *mongodbatlas.CloudProviderSnapshots {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshots, 10)
	var innerWg sync.WaitGroup
	go func() {
		defer func() {
			innerWg.Wait()
			log.Debug().Msg("Mongo: Snapshots Aggregator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for i, in := range inputs {
			innerWg.Add(1)
			log.Debug().Msgf("Mongo: Snapshots Aggregator %d processing working!", i+1)
			go func(index int, in <-chan *mongodbatlas.CloudProviderSnapshots) {
				defer innerWg.Done()
				log.Debug().Msgf("Mongo: Snapshots Aggregator %d Inner Func processing working!", index)
				for x := range in {
					output <- x
				}
			}(i+1, in)
		}
	}()
	return output
}

func snapshotsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshots) <-chan *mongodbatlas.CloudProviderSnapshot {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshot, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Snapshots Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for snapshots := range input {
			log.Debug().Msg("Snapshots Mapper processing working!")
			time.Sleep(time.Second)
			for _, snapshot := range snapshots.Results {
				select {
				case output <- snapshot:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func snapshotFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshot) <-chan *mongodbatlas.CloudProviderSnapshot {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshot, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Snapshots Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for snapshot := range input {
			log.Debug().Msg("Snapshots Filter processing working!")
			time.Sleep(time.Second)

			if snapshot == nil || snapshot.ID == "" {
				break
			}

			select {
			case output <- snapshot:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func snapshotPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshot) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Snapshot Printer exit")
			wg.Done()
		}()

		for snapshot := range input {
			log.Debug().Msg("Mongo: Snapshot Printer processing working!")
			log.Info().Msgf("Mongo: Snapshot: %+v", snapshot)
		}
	}()
}
