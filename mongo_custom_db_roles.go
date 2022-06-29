package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func CustomDbRolesStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan *[]mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *[]mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Custom Db Roles Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for project := range input {
			time.Sleep(time.Second)
			customDBRoles, _, err := client.CustomDBRoles.List(ctx, project.ID, nil)
			if err != nil || customDBRoles == nil || len(*customDBRoles) == 0 {
				break
			}
			output <- customDBRoles
		}
	}()
	return output
}

func CustomDbRolesMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *[]mongodbatlas.CustomDBRole) <-chan mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Custom Db Roles Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for customDBRoles := range input {
			log.Debug().Msg("Custom Db Roles Mapper processing working!")
			time.Sleep(time.Second)
			for _, customDBRole := range *customDBRoles {
				output <- customDBRole
			}
		}
	}()
	return output
}

func CustomDbRoleFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.CustomDBRole) <-chan mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Custom Db Role Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for customDBRole := range input {
			log.Debug().Msg("Custom Db Role Filter processing working!")
			time.Sleep(time.Second)
			if customDBRole.RoleName != "" {
				output <- customDBRole
			}
		}
	}()
	return output
}

func CustomDbRolePrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.CustomDBRole) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for customDBRole := range input {
			log.Debug().Msg("Custom Db Role Printer processing working!")
			log.Info().Msgf("\tRole: Name %v", customDBRole.RoleName)
		}
	}()
}
