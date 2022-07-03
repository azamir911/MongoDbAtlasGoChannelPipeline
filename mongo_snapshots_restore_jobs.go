package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func SnapshotsRestoreJobsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.AdvancedCluster) <-chan *mongodbatlas.CloudProviderSnapshotRestoreJobs {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshotRestoreJobs, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Restore Jobs Closing channel output!")
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
				ItemsPerPage: 1,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				snapshotRestoreJobs, _, err := client.CloudProviderSnapshotRestoreJobs.List(ctx, snapshotReqPathParameters, options)
				if err != nil {
					log.Err(err).Msg("Failed to get cloud provider snapshot restore jobs list")
					break
				}
				if snapshotRestoreJobs == nil || len(snapshotRestoreJobs.Results) == 0 {
					break
				}

				select {
				case output <- snapshotRestoreJobs:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func SnapshotsRestoreJobsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshotRestoreJobs) <-chan *mongodbatlas.CloudProviderSnapshotRestoreJob {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshotRestoreJob, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Restore Jobs Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for snapshotRestoreJobs := range input {
			log.Debug().Msg("Snapshots Restore Jobs Mapper processing working!")
			time.Sleep(time.Second)
			for _, snapshotRestoreJob := range snapshotRestoreJobs.Results {
				select {
				case output <- snapshotRestoreJob:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func SnapshotRestoreJobFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshotRestoreJob) <-chan *mongodbatlas.CloudProviderSnapshotRestoreJob {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.CloudProviderSnapshotRestoreJob, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Snapshots Restore Job Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for snapshotRestoreJob := range input {
			log.Debug().Msg("Snapshots Restore Job Filter processing working!")
			time.Sleep(time.Second)

			if snapshotRestoreJob == nil || snapshotRestoreJob.ID == "" {
				break
			}

			select {
			case output <- snapshotRestoreJob:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func SnapshotRestoreJobPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.CloudProviderSnapshotRestoreJob) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Snapshot Restore Job Printer exit")
			wg.Done()
		}()

		for snapshotRestoreJob := range input {
			log.Debug().Msg("Snapshot Restore Job Printer processing working!")
			log.Info().Msgf("Snapshot Restore Job: %+v", snapshotRestoreJob)
		}
	}()
}
