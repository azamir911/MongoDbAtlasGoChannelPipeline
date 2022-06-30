package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func ClustersStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan *mongodbatlas.AdvancedClustersResponse {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AdvancedClustersResponse, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Clusters Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for project := range input {
			// Declare the option to get only one team id
			options := &mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 1,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				advancedClustersResponse, _, err := client.AdvancedClusters.List(ctx, project.ID, options)
				if err != nil || advancedClustersResponse == nil || len(advancedClustersResponse.Results) == 0 {
					break
				}

				select {
				case output <- advancedClustersResponse:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func ClustersMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AdvancedClustersResponse) <-chan *mongodbatlas.AdvancedCluster {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AdvancedCluster, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Clusters Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for advancedClustersResponse := range input {
			log.Debug().Msg("Clusters Mapper processing working!")
			time.Sleep(time.Second)
			for _, advancedCluster := range advancedClustersResponse.Results {
				select {
				case output <- advancedCluster:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func ClusterDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AdvancedCluster) (<-chan *mongodbatlas.AdvancedCluster, <-chan *mongodbatlas.AdvancedCluster, <-chan *mongodbatlas.AdvancedCluster, <-chan *mongodbatlas.AdvancedCluster) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	outputA, outputB, outputC, outputD := make(chan *mongodbatlas.AdvancedCluster, 10), make(chan *mongodbatlas.AdvancedCluster, 10), make(chan *mongodbatlas.AdvancedCluster, 10), make(chan *mongodbatlas.AdvancedCluster, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Cluster Duplicator Closing channel outputA and outputB!")
			close(outputA)
			close(outputB)
			close(outputC)
			close(outputD)
			wg.Done()
		}()

		for advancedCluster := range input {
			log.Debug().Msg("Cluster Duplicator processing working!")
			time.Sleep(time.Second)

			select {
			case outputA <- advancedCluster:
			case <-ctx.Done():
				return
			}

			select {
			case outputB <- advancedCluster:
			case <-ctx.Done():
				return
			}

			select {
			case outputC <- advancedCluster:
			case <-ctx.Done():
				return
			}

			select {
			case outputD <- advancedCluster:
			case <-ctx.Done():
				return
			}
		}
	}()
	return outputA, outputB, outputC, outputD
}

func ClusterPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AdvancedCluster) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for cluster := range input {
			log.Debug().Msg("Cluster Printer processing working!")
			log.Info().Msgf("\tAdvanced Cluster: %+v", cluster)
		}
	}()
}
