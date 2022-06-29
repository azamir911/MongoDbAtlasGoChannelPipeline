package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func TeamsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Organization) <-chan []mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
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
				if err != nil || teams == nil || len(teams) == 0 {
					break
				}
				output <- teams
				options.PageNum++
			}
		}
	}()
	return output
}

func TeamsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan []mongodbatlas.Team) <-chan mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Teams Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case teams, ok := <-input:
				if !ok {
					log.Debug().Msg("Teams Mapper processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Teams Mapper processing working!")
					time.Sleep(time.Second)
					for _, team := range teams {
						output <- team
					}
				}
			}
		}
	}()
	return output
}

func TeamFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.Team) <-chan mongodbatlas.Team {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.Team, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Team Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case team, ok := <-input:
				if !ok {
					log.Debug().Msg("Teams Filter processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Teams Filter processing working!")
					time.Sleep(time.Second)
					if team.ID != "" {
						output <- team
					}
				}
			}
		}
	}()
	return output
}

func TeamPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.Team) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for {
			select {
			case team, ok := <-input:
				if !ok {
					log.Debug().Msg("Team Printer processing exist!")
					input = nil
					return
				}
				//time.Sleep(time.Second)
				log.Debug().Msg("Team Printer processing working!")
				log.Info().Msgf("\tTeam: ID '%v', Name '%v'", team.ID, team.Name)
			}
		}
	}()
}
