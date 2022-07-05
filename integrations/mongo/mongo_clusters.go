package mongo

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"strings"
	"sync"
	"time"
)

type ClustersWithTeams struct {
	Clusters      *mongodbatlas.AdvancedClustersResponse
	AssignedTeams []*mongodbatlas.Result
}

type ClusterWithTeams struct {
	Cluster       *mongodbatlas.AdvancedCluster
	AssignedTeams []*mongodbatlas.Result
}

func clustersWithTeamsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *ProjectWithTeams) <-chan *ClustersWithTeams {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *ClustersWithTeams, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Clusters With Teams Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for projectWithTeams := range input {
			// Declare the option to get only one team id
			options := &mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 1,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				advancedClustersResponse, _, err := client.AdvancedClusters.List(ctx, projectWithTeams.Project.ID, options)
				if err != nil {
					log.Err(err).Msg("Failed to get advanced clusters list")
					break
				}
				if advancedClustersResponse == nil || len(advancedClustersResponse.Results) == 0 {
					break
				}

				clustersWithTeams := &ClustersWithTeams{
					Clusters:      advancedClustersResponse,
					AssignedTeams: projectWithTeams.AssignedTeams,
				}

				select {
				case output <- clustersWithTeams:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func clustersWithTeamsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *ClustersWithTeams) <-chan *ClusterWithTeams {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *ClusterWithTeams, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Clusters With Teams Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for clustersWithTeams := range input {
			log.Debug().Msg("Clusters With Teams Mapper processing working!")
			time.Sleep(time.Second)
			for _, advancedCluster := range clustersWithTeams.Clusters.Results {

				clusterWithTeams := &ClusterWithTeams{
					Cluster:       advancedCluster,
					AssignedTeams: clustersWithTeams.AssignedTeams,
				}
				select {
				case output <- clusterWithTeams:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func clusterWithTeamsDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *ClusterWithTeams) (<-chan *ClusterWithTeams, <-chan *ClusterWithTeams) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	outputA, outputB := make(chan *ClusterWithTeams, 10), make(chan *ClusterWithTeams, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Cluster With Teams Duplicator Closing channel outputA and outputB!")
			close(outputA)
			close(outputB)
			wg.Done()
		}()

		for advancedCluster := range input {
			log.Debug().Msg("Cluster With Teams Duplicator processing working!")
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
		}
	}()
	return outputA, outputB
}

func clusterWithTeamsPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *ClusterWithTeams) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for clusterWithTeams := range input {
			log.Debug().Msg("Cluster With Teams Printer processing working!")
			var assignedRoles string
			var assignedTeams string
			if clusterWithTeams != nil && clusterWithTeams.AssignedTeams != nil && len(clusterWithTeams.AssignedTeams) > 0 {
				for _, team := range clusterWithTeams.AssignedTeams {
					assignedRoles = assignedRoles + strings.Join(team.RoleNames, `','`)
					assignedTeams = assignedTeams + ` ` + team.TeamID
				}

			}

			log.Info().Msgf("\tAdvanced Cluster: %+v, Teams Assigned: %+v, Teams Roles: %+v", clusterWithTeams.Cluster, assignedTeams, assignedRoles)
		}
	}()
}

func clusterWithTeamsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *ClusterWithTeams) <-chan *mongodbatlas.AdvancedCluster {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AdvancedCluster, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Cluster With Teams Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for clustersWithTeams := range input {
			log.Debug().Msg("Cluster With Teams Mapper processing working!")
			time.Sleep(time.Second)

			select {
			case output <- clustersWithTeams.Cluster:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func clusterDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AdvancedCluster) (<-chan *mongodbatlas.AdvancedCluster, <-chan *mongodbatlas.AdvancedCluster, <-chan *mongodbatlas.AdvancedCluster) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	outputB, outputC, outputD := make(chan *mongodbatlas.AdvancedCluster, 10), make(chan *mongodbatlas.AdvancedCluster, 10), make(chan *mongodbatlas.AdvancedCluster, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Cluster Duplicator Closing channel outputA and outputB!")
			close(outputB)
			close(outputC)
			close(outputD)
			wg.Done()
		}()

		for advancedCluster := range input {
			log.Debug().Msg("Cluster Duplicator processing working!")
			time.Sleep(time.Second)

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
	return outputB, outputC, outputD
}
