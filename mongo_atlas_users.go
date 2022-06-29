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
				if err != nil || atlasUsersResponse == nil || len(atlasUsersResponse.Results) == 0 {
					break
				}
				output <- atlasUsersResponse
				options.PageNum++
			}
		}
	}()
	return output
}

//func AtlasUsersResponseFilter(_ context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUsersResponse) <-chan *mongodbatlas.AtlasUsersResponse {
//	wg.Add(1)
//	output := make(chan *mongodbatlas.AtlasUsersResponse, 10)
//
//	go func() {
//		defer func() {
//			fmt.Println("Atlas User Response Filter Closing channel output!")
//			close(output)
//			wg.Done()
//		}()
//
//		for {
//			select {
//			case atlasUsersResponse, ok := <-input:
//				if !ok {
//					fmt.Println("Atlas User Filter processing exist!")
//					input = nil
//					return
//				} else if atlasUsersResponse != nil && len(atlasUsersResponse.Results) > 0 {
//					fmt.Println("Atlas User Filter processing working!")
//					time.Sleep(time.Second)
//					output <- atlasUsersResponse
//				}
//			}
//		}
//	}()
//	return output
//}
//
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

		for {
			select {
			case atlasUsersResponse, ok := <-input:
				if !ok {
					log.Debug().Msg("Atlas User Mapper processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Atlas User Mapper processing working!")
					time.Sleep(time.Second)
					for _, atlasUser := range atlasUsersResponse.Results {
						output <- atlasUser
					}
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

		for {
			select {
			case atlasUser, ok := <-input:
				if !ok {
					log.Debug().Msg("Atlas User Filter processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Atlas User Filter processing working!")
					time.Sleep(time.Second)
					if atlasUser.ID != "" {
						output <- atlasUser
					}
				}
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

		for {
			select {
			case user, ok := <-input:
				if !ok {
					log.Debug().Msg("Atlas User Printer processing exist!")
					input = nil
					return
				}
				//time.Sleep(time.Second)
				log.Debug().Msg("Atlas User Printer processing working!")
				log.Info().Msgf("\tAtlas User: Id %v, FirstName %v, LastName %v, Username %v", user.ID, user.FirstName, user.LastName, user.Username)
			}
		}
	}()
}
