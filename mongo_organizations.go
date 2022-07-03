package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	//"log"
	"sync"
	"time"
)

func OrganizationsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client) <-chan *mongodbatlas.Organizations {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organizations, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Organizations Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

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
				log.Err(err).Msg("Failed to get organizations list")
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

func OrganizationsFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organizations) <-chan *mongodbatlas.Organizations {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organizations, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Organizations Filter Closing channel output!")
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

func OrganizationsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organizations) <-chan *mongodbatlas.Organization {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Organization, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Organizations Mapper Closing channel output!")
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

func OrganizationsDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organization) (<-chan *mongodbatlas.Organization, <-chan *mongodbatlas.Organization, <-chan *mongodbatlas.Organization) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	outputA, outputB, outputC := make(chan *mongodbatlas.Organization, 10), make(chan *mongodbatlas.Organization, 10), make(chan *mongodbatlas.Organization, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Organizations Duplicator Closing channel outputA and outputB!")
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

func OrganizationPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Organization) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer func() {
			log.Debug().Msg("Organization Printer exit")
			wg.Done()
		}()

		for organization := range input {
			log.Debug().Msg("Organization Printer processing working!")
			log.Info().Msgf("\tOrg: Id %v, Name %v", organization.ID, organization.Name)
		}
	}()
}
