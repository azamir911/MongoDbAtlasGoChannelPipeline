package elastic

import (
	elasticConnector "MongoDbAtlasGoChannelPipeline/integrations/elastic/connector"
	"context"
	"github.com/rs/zerolog"
	"sync"
)

func elasticCloudUsersStreamer(ctx context.Context, wg *sync.WaitGroup, connector elasticConnector.ElasticCloudConnector, input <-chan string) <-chan *elasticConnector.OrganizationMemberships {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *elasticConnector.OrganizationMemberships, 1)

	go func() {
		defer func() {
			log.Debug().Msg("Elastic: Cloud User Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organization := range input {
			//time.Sleep(time.Second)
			members, err := connector.GetMembers(ctx, organization)
			if err != nil {
				log.Err(err).Msg("Elastic: Failed to get elastic cloud members")
				return
			}
			if members == nil {
				return
			}

			select {
			case output <- members:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

func elasticCloudUsersMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *elasticConnector.OrganizationMemberships) <-chan *elasticConnector.OrganizationMembership {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *elasticConnector.OrganizationMembership, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Elastic: Cloud User Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organizationMemberships := range input {
			log.Debug().Msg("Elastic: Cloud User Mapper processing working!")
			//time.Sleep(time.Second)
			for _, organizationMembership := range organizationMemberships.Members {
				organizationMembership := organizationMembership
				select {
				case output <- &organizationMembership:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output
}

func elasticCloudUserFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *elasticConnector.OrganizationMembership) <-chan *elasticConnector.OrganizationMembership {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *elasticConnector.OrganizationMembership, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Elastic: Cloud User Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organizationMembership := range input {
			log.Debug().Msg("Elastic: Cloud User Filter processing working!")
			//time.Sleep(time.Second)

			if organizationMembership.UserId == "" {
				break
			}
			select {
			case output <- organizationMembership:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

func elasticCloudUserPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *elasticConnector.OrganizationMembership) <-chan *elasticConnector.OrganizationMembership {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *elasticConnector.OrganizationMembership, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Elastic: Cloud User Printer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for organizationMembership := range input {
			log.Debug().Msg("Elastic: Cloud User Printer processing working!")
			//time.Sleep(time.Second)

			log.Info().Msgf("\tElastic: Cloud User: Id %v, Name %v, Email %v, MemberSince %v", organizationMembership.UserId, organizationMembership.Name, organizationMembership.Email, organizationMembership.MemberSince)

			select {
			case output <- organizationMembership:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}
