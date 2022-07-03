package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func AtlasUsersStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Organization) <-chan *mongodbatlas.AtlasUsersResponse {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AtlasUsersResponse, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas User Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organization := range input {
			// Declare the option to get only one organization id
			options := &mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 1,
				IncludeCount: false,
			}

			for {
				time.Sleep(time.Second)
				atlasUsersResponse, _, err := client.Organizations.Users(ctx, organization.ID, options)
				if err != nil {
					log.Err(err).Msg("Failed to get organizations users")
					break
				}
				if atlasUsersResponse == nil || len(atlasUsersResponse.Results) == 0 {
					break
				}

				select {
				case output <- atlasUsersResponse:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func AtlasUsersResponseMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUsersResponse) <-chan mongodbatlas.AtlasUser {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.AtlasUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas User Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for atlasUsersResponse := range input {
			log.Debug().Msg("Atlas User Mapper processing working!")
			time.Sleep(time.Second)
			for _, atlasUser := range atlasUsersResponse.Results {
				select {
				case output <- atlasUser:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func AtlasUsersFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.AtlasUser) <-chan mongodbatlas.AtlasUser {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.AtlasUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas User Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for atlasUser := range input {
			log.Debug().Msg("Atlas User Filter processing working!")
			time.Sleep(time.Second)
			if atlasUser.ID == "" {
				break
			}

			select {
			case output <- atlasUser:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func AtlasUserPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.AtlasUser) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for user := range input {
			log.Debug().Msg("Atlas User Printer processing working!")
			log.Info().Msgf("\tAtlas User: Id %v, FirstName %v, LastName %v, Username %v", user.ID, user.FirstName, user.LastName, user.Username)
		}
	}()
}
