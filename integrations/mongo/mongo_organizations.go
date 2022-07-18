package mongo

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func organizationsStreamer(ctx context.Context, wg *sync.WaitGroup) <-chan *mongodbatlas.Organizations {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organizations, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Organizations Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		client := newClient()

		// Declare the option to get only one organization id
		options := &mongodbatlas.OrganizationsListOptions{
			ListOptions: mongodbatlas.ListOptions{
				PageNum:      1,
				ItemsPerPage: 1,
				IncludeCount: false,
			},
		}

		for {
			time.Sleep(time.Second)
			organizations, _, err := client.Organizations.List(ctx, options)
			if err != nil {
				log.Err(err).Msg("Mongo: Failed to get organizations list")
				break
			}
			if organizations == nil || len(organizations.Results) == 0 {
				break
			}

			select {
			case output <- organizations:
			case <-ctx.Done():
				return
			}

			options.PageNum++
		}
	}()
	return output
}

func organizationsFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organizations) <-chan *mongodbatlas.Organizations {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organizations, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Organizations Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organizations := range input {
			log.Debug().Msg("Organizations Filter processing working!")
			time.Sleep(time.Second)
			select {
			case output <- organizations:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func organizationsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organizations) <-chan *mongodbatlas.Organization {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organization, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Organizations Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organizations := range input {
			log.Debug().Msg("Organizations Mapper processing working!")
			time.Sleep(time.Second)
			for _, organization := range organizations.Results {
				select {
				case output <- organization:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func organizationDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organization) (<-chan *mongodbatlas.Organization, <-chan *mongodbatlas.Organization, <-chan *mongodbatlas.Organization) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	outputA, outputB, outputC := make(chan *mongodbatlas.Organization, 10), make(chan *mongodbatlas.Organization, 10), make(chan *mongodbatlas.Organization, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Organizations Duplicator Closing channel outputA and outputB!")
			close(outputA)
			close(outputB)
			close(outputC)
			wg.Done()
		}()

		for organization := range input {
			log.Debug().Msg("Organizations Duplicator processing working!")
			time.Sleep(time.Second)
			select {
			case outputA <- organization:
			case <-ctx.Done():
				return
			}

			select {
			case outputB <- organization:
			case <-ctx.Done():
				return
			}

			select {
			case outputC <- organization:
			case <-ctx.Done():
				return
			}
		}
	}()
	return outputA, outputB, outputC
}

func organizationPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organization) {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Organization Printer exit")
			wg.Done()
		}()

		for organization := range input {
			log.Debug().Msg("Mongo: Organization Printer processing working!")
			log.Info().Msgf("\tMongo: Org: Id %v, Name %v", organization.ID, organization.Name)
		}
	}()
}
