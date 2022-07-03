package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func SnapshotsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.AdvancedCluster) <-chan *mongodbatlas.CloudProviderSnapshots {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshots, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Closing channel output!")
			close(output)
			wg.Done()
		}()

		for cluster := range input {
			snapshotReqPathParameters := &mongodbatlas.SnapshotReqPathParameters{
				GroupID:     cluster.GroupID,
				ClusterName: cluster.Name,
			}

			// Declare the option to get only one team id
			options := &mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 3,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				snapshots, _, err := client.CloudProviderSnapshots.GetAllCloudProviderSnapshots(ctx, snapshotReqPathParameters, options)
				if err != nil {
					log.Err(err).Msg("Failed to get cloud provider snapshots list")
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

				options.PageNum++
			}
		}
	}()
	return output
}

func SnapshotsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshots) <-chan *mongodbatlas.CloudProviderSnapshot {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshot, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Mapper Closing channel output!")
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

func SnapshotFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshot) <-chan *mongodbatlas.CloudProviderSnapshot {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshot, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Filter Closing channel output!")
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

func SnapshotPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshot) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Snapshot Printer exit")
			wg.Done()
		}()

		for snapshot := range input {
			log.Debug().Msg("Snapshot Printer processing working!")
			log.Info().Msgf("Snapshot: %+v", snapshot)
		}
	}()
}
