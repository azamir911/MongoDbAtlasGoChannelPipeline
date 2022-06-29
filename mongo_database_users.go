package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func DatabaseUsersStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan []mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan []mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database Users Streamer Closing channel output!")
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
				databaseUsers, _, err := client.DatabaseUsers.List(ctx, project.ID, options)
				if err != nil || databaseUsers == nil || len(databaseUsers) == 0 {
					break
				}
				output <- databaseUsers
				options.PageNum++
			}
		}
	}()
	return output
}

func DatabaseUsersMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan []mongodbatlas.DatabaseUser) <-chan mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database Users Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case databaseUsers, ok := <-input:
				if !ok {
					log.Debug().Msg("Database Users Mapper processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Database Users Mapper processing working!")
					time.Sleep(time.Second)
					for _, databaseUser := range databaseUsers {
						output <- databaseUser
					}
				}
			}
		}
	}()
	return output
}

func DatabaseUserFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.DatabaseUser) <-chan mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database User Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case databaseUser, ok := <-input:
				if !ok {
					log.Debug().Msg("Database User Filter processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Database User Filter processing working!")
					time.Sleep(time.Second)
					if databaseUser.Username != "" {
						output <- databaseUser
					}
				}
			}
		}
	}()
	return output
}

func DatabaseUserPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.DatabaseUser) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for {
			select {
			case databaseUser, ok := <-input:
				if !ok {
					log.Debug().Msg("Database User Printer processing exist!")
					input = nil
					return
				}
				//time.Sleep(time.Second)
				log.Debug().Msg("Database User Printer processing working!")
				log.Info().Msgf("\tDatabase User: Username %v", databaseUser.Username)
			}
		}
	}()
}