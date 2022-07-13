package elastic

import (
	elasticConnector "MongoDbAtlasGoChannelPipeline/integrations/elastic/connector"
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"sync"
	"time"
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

var userStatusActive = assetdata_model.UserStatusActive

func normalizedElasticCloudUserCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *elasticConnector.OrganizationMembership) <-chan *assetdata_model.User {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.User, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Elastic: Normalized Cloud User Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Elastic: Normalized Cloud User Creator processing working!")

			isAdmin := true
			createdAt, _ := extractElasticUserCreatedAt(user)

			normalizedUser := &assetdata_model.User{
				AssetDataBaseFields: assetdata_model.AssetDataBaseFields{
					ID:          user.UserId,
					Name:        &user.Name,
					Integration: 57, //int(common.ELASTIC),
					Account:     "uniqueKey",
				},
				Emails:    []string{user.Email},
				CreatedAt: createdAt,
				IsAdmin:   &isAdmin,
				Status:    &userStatusActive,
			}

			select {
			case output <- normalizedUser:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

func extractElasticUserCreatedAt(user *elasticConnector.OrganizationMembership) (*time.Time, error) {
	createdAt, err := time.Parse(time.RFC3339, user.MemberSince)
	if err != nil {
		return nil, err
	}

	return &createdAt, nil
}

func normalizedUserAssetCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.User) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Mongo: Atlas Normalized User Asset Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Mongo: Atlas Normalized User Asset Creator processing working!")

			asset := &assetdata_model.NormalizedAsset{
				Id:            uuid.New(),
				AccountId:     "uniqueConnectionKey",
				IntegrationId: 57, //common.ELASTIC,
				Type:          assetdata_model.UserAssetType,
				Data:          user,
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
