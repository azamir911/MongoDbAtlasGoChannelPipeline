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
				output <- advancedClustersResponse
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

		for {
			select {
			case advancedClustersResponse, ok := <-input:
				if !ok {
					log.Debug().Msg("Clusters Mapper processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Clusters Mapper processing working!")
					time.Sleep(time.Second)
					for _, advancedCluster := range advancedClustersResponse.Results {
						output <- advancedCluster
					}
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

		for {
			select {
			case advancedCluster, ok := <-input:
				if !ok {
					log.Debug().Msg("Cluster Duplicator processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Cluster Duplicator processing working!")
					time.Sleep(time.Second)
					outputA <- advancedCluster
					outputB <- advancedCluster
					outputC <- advancedCluster
					outputD <- advancedCluster
				}
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

		for {
			select {
			case cluster, ok := <-input:
				if !ok {
					log.Debug().Msg("Cluster Printer processing exist!")
					input = nil
					return
				}
				//time.Sleep(time.Second)
				log.Debug().Msg("Cluster Printer processing working!")
				log.Info().Msgf("\tAdvanced Cluster: %+v", cluster)
			}
		}
	}()
}
