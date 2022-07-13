package mongo

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func customDbRolesStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan *[]mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *[]mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Custom Db Roles Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for project := range input {
			time.Sleep(time.Second)
			customDBRoles, _, err := client.CustomDBRoles.List(ctx, project.ID, nil)
			if err != nil {
				log.Err(err).Msg("Mongo: Failed to get custom DB roles list")
				break
			}
			if customDBRoles == nil || len(*customDBRoles) == 0 {
				break
			}

			select {
			case output <- customDBRoles:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func customDbRolesMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *[]mongodbatlas.CustomDBRole) <-chan mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Custom Db Roles Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for customDBRoles := range input {
			log.Debug().Msg("Custom Db Roles Mapper processing working!")
			time.Sleep(time.Second)
			for _, customDBRole := range *customDBRoles {
				select {
				case output <- customDBRole:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func customDbRoleFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.CustomDBRole) <-chan mongodbatlas.CustomDBRole {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan mongodbatlas.CustomDBRole, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Custom Db Role Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for customDBRole := range input {
			log.Debug().Msg("Custom Db Role Filter processing working!")
			time.Sleep(time.Second)
			if customDBRole.RoleName != "" {
				select {
				case output <- customDBRole:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func customDbRolePrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan mongodbatlas.CustomDBRole) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for customDBRole := range input {
			log.Debug().Msg("Mongo: Custom Db Role Printer processing working!")
			log.Info().Msgf("\tMongo: Role: Name %v", customDBRole.RoleName)
		}
	}()
}
