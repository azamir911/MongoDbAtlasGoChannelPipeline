package mongo

import (
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func teamsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Organization) <-chan []mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan []mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Teams Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organization := range input {
			// Declare the option to get only one team id
			options := &mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 1,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				teams, _, err := client.Teams.List(ctx, organization.ID, options)
				if err != nil {
					log.Err(err).Msg("Failed to get teams list")
					break
				}
				if teams == nil || len(teams) == 0 {
					break
				}

				select {
				case output <- teams:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func teamsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan []mongodbatlas.Team) <-chan *mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Teams Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for teams := range input {
			log.Debug().Msg("Teams Mapper processing working!")
			time.Sleep(time.Second)
			for _, team := range teams {
				team := team
				select {
				case output <- &team:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func teamFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Team) <-chan *mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Team Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for team := range input {
			log.Debug().Msg("Teams Filter processing working!")
			time.Sleep(time.Second)
			if team.ID != "" {
				select {
				case output <- team:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func teamPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Team) <-chan *mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas User Printer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for team := range input {
			log.Debug().Msg("Team Printer processing working!")
			log.Info().Msgf("\tTeam: ID '%v', Name '%v'", team.ID, team.Name)

			select {
			case output <- team:
			case <-ctx.Done():
				return
			}

		}
	}()

	return output
}

func normalizedAtlasTeamCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Team) <-chan *assetdata_model.Group {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.Group, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Normalized Atlas Group Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for team := range input {
			log.Debug().Msg("Normalized Atlas Group Creator processing working!")

			normalizedGroup := &assetdata_model.Group{
				AssetDataBaseFields: assetdata_model.AssetDataBaseFields{
					ID:          team.ID,
					Name:        &team.Name,
					Integration: 60, //int(common.MONGODB_ATLAS),
					Account:     "uniqueKey",
				},
				RoleIds:     nil, // Roles will be assigned to team from the project cluster
				PolicyIds:   nil, // Team does not have policies
				Permissions: nil, // Team does not have permissions
			}

			select {
			case output <- normalizedGroup:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

func normalizedTeamAssetCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.Group) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas Normalized Team Asset Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for group := range input {
			log.Debug().Msg("Atlas Normalized Team Asset Creator processing working!")

			asset := &assetdata_model.NormalizedAsset{
				Id:            uuid.New(),
				AccountId:     "uniqueConnectionKey",
				IntegrationId: 60, //common.MONGODB_ATLAS,
				Type:          assetdata_model.GroupAssetType,
				Data:          group,
			}

			select {
			case output <- asset:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}
