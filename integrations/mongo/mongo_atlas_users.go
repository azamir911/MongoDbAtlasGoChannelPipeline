package mongo

import (
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"strings"
	"sync"
	"time"
)

func atlasUsersStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Organization) <-chan *mongodbatlas.AtlasUsersResponse {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
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

func atlasUsersResponseMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUsersResponse) <-chan *mongodbatlas.AtlasUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AtlasUser, 10)

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
				atlasUser := atlasUser
				select {
				case output <- &atlasUser:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func atlasUserFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUser) <-chan *mongodbatlas.AtlasUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AtlasUser, 10)

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

func atlasUserPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUser) <-chan *mongodbatlas.AtlasUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.AtlasUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas User Printer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Atlas User Printer processing working!")
			log.Info().Msgf("\tAtlas User: Id %v, FirstName %v, LastName %v, Username %v", user.ID, user.FirstName, user.LastName, user.Username)

			select {
			case output <- user:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

func normalizedAtlasUserCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.AtlasUser) <-chan *assetdata_model.User {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.User, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Normalized Atlas User Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Normalized Atlas User Creator processing working!")

			groupIds := extractAtlasUserGroupIds(user)
			roles, isAdmin := extractAtlasUserRoles(user)

			// Name is a concatenation of first name and last name
			name := strings.TrimSpace(fmt.Sprintf("%s %s", user.FirstName, user.LastName))

			normalizedUser := &assetdata_model.User{
				AssetDataBaseFields: assetdata_model.AssetDataBaseFields{
					ID:          user.ID,
					Name:        &name,
					Integration: 60, //int(common.MONGODB_ATLAS),
					Account:     "uniqueKey",
				},
				Emails:   []string{user.EmailAddress},
				IsAdmin:  &isAdmin,
				Status:   &userStatusActive, // Atlas User is always active
				GroupIds: groupIds,
				RoleIds:  roles,
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

func extractAtlasUserGroupIds(user *mongodbatlas.AtlasUser) (groupIds []string) {
	for i := 0; i < len(user.TeamIds); i++ {
		groupIds = append(groupIds, user.TeamIds[i])
	}

	return groupIds
}

const (
	atlasUserAdminRole = "ORG_OWNER"
)

var userStatusActive = assetdata_model.UserStatusActive

func extractAtlasUserRoles(user *mongodbatlas.AtlasUser) (roles []assetdata_model.RBACScopePair, isAdmin bool) {
	for _, role := range user.Roles {
		var resource string
		if role.OrgID == "" {
			resource = role.GroupID
		} else {
			resource = role.OrgID
		}
		roles = append(roles, assetdata_model.RBACScopePair{
			Id: role.RoleName,
			Scope: &assetdata_model.RBACScope{
				Type:   nil, // Currently, we don't collect both project assets and account asset. Therefore, resource entity stays as ID
				Entity: resource,
			},
		})

		if !isAdmin && role.RoleName == atlasUserAdminRole {
			isAdmin = true
		}
	}

	return roles, isAdmin
}

func normalizedUserAssetCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *assetdata_model.User) <-chan *assetdata_model.NormalizedAsset {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.NormalizedAsset, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Atlas Normalized User Asset Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Atlas Normalized User Asset Creator processing working!")

			asset := &assetdata_model.NormalizedAsset{
				Id:            uuid.New(),
				AccountId:     "uniqueConnectionKey",
				IntegrationId: 60, //common.MONGODB_ATLAS,
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
