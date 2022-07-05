package mongo

import (
	"MongoDbAtlasGoChannelPipeline/pkg/model/assetdata_model"
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func databaseUsersStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan []mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
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
				if err != nil {
					log.Err(err).Msg("Failed to get database users list")
					break
				}
				if databaseUsers == nil || len(databaseUsers) == 0 {
					break
				}

				select {
				case output <- databaseUsers:
				case <-ctx.Done():
					return
				}

				options.PageNum++
			}
		}
	}()
	return output
}

func databaseUsersMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan []mongodbatlas.DatabaseUser) <-chan *mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database Users Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for databaseUsers := range input {
			log.Debug().Msg("Database Users Mapper processing working!")
			time.Sleep(time.Second)
			for _, databaseUser := range databaseUsers {
				select {
				case output <- &databaseUser:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return output
}

func databaseUserFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.DatabaseUser) <-chan *mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database User Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for databaseUser := range input {
			log.Debug().Msg("Database User Filter processing working!")
			time.Sleep(time.Second)
			if databaseUser.Username == "" {
				break
			}

			select {
			case output <- databaseUser:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func databaseUserPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.DatabaseUser) <-chan *mongodbatlas.DatabaseUser {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.DatabaseUser, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Database User Printer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for databaseUser := range input {
			log.Debug().Msg("Database User Printer processing working!")
			log.Info().Msgf("\tDatabase User: %v", databaseUser.Username)

			select {
			case output <- databaseUser:
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}

const (
	databaseUserAdminRole = "atlasAdmin"
)

func normalizedDatabaseUserCreator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.DatabaseUser) <-chan *assetdata_model.User {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *assetdata_model.User, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Normalized Database User Creator Closing channel output!")
			close(output)
			wg.Done()
		}()

		for user := range input {
			log.Debug().Msg("Normalized Database User Creator processing working!")

			// Name is a concatenation of a project and a cluster
			//name := fmt.Sprintf("%s/%s", project.Name, user.Username)
			name := fmt.Sprintf("%s", user.Username)

			roles, isAdmin := extractDatabaseUserRoles(user)

			normalizedUser := &assetdata_model.User{
				AssetDataBaseFields: assetdata_model.AssetDataBaseFields{
					ID:          name,
					Name:        &name,
					Integration: 60, //int(common.MONGODB_ATLAS),
					Account:     "uniqueKey",
				},
				IsAdmin:     &isAdmin,
				Status:      &userStatusActive, // Database User is always active
				GroupIds:    nil,               // Database User does not has groups
				RoleIds:     roles,
				PolicyIds:   nil, // Database User does not has policies
				Permissions: nil, // Database User does not has permissions
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

func extractDatabaseUserRoles(user *mongodbatlas.DatabaseUser) (roles []assetdata_model.RBACScopePair, isAdmin bool) {
	for _, role := range user.Roles {
		id := fmt.Sprintf("%s", role.RoleName)
		resource := getRoleResourceName(role)
		roles = append(roles, assetdata_model.RBACScopePair{
			Id: id,
			Scope: &assetdata_model.RBACScope{
				Type:   nil, // Database resource entity is a custom alias name (provided by the user)
				Entity: resource,
			},
		})

		if !isAdmin && role.RoleName == databaseUserAdminRole {
			isAdmin = true
		}
	}

	return roles, isAdmin
}

func getRoleResourceName(role mongodbatlas.Role) string {
	isDbExists := role.DatabaseName != ""
	isCollectionExists := role.CollectionName != ""

	if isDbExists && isCollectionExists {
		return fmt.Sprintf("%s/%s", role.DatabaseName, role.CollectionName)
	}
	if isDbExists {
		return fmt.Sprintf("%s", role.DatabaseName)
	}
	if isCollectionExists {
		return fmt.Sprintf("%s", role.CollectionName)
	}

	return ""
}
