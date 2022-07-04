package mongo

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

type ProjectWithTeams struct {
	Project       *mongodbatlas.Project
	AssignedTeams []*mongodbatlas.Result
}

func teamsAssignedStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client, input <-chan *mongodbatlas.Project) <-chan *ProjectWithTeams {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan *ProjectWithTeams, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Teams Assigned Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		for project := range input {
			log.Debug().Msg("Teams Assigned processing working!")
			time.Sleep(time.Second)
			teamsAssigned, _, err := client.Projects.GetProjectTeamsAssigned(ctx, project.ID)

			if err != nil {
				log.Err(err).Msg("Failed to get teams assigned list")
				break
			}

			var assignedTeams []*mongodbatlas.Result
			if teamsAssigned != nil {
				assignedTeams = teamsAssigned.Results
			}

			projectWithTeams := &ProjectWithTeams{
				Project:       project,
				AssignedTeams: assignedTeams,
			}

			select {
			case output <- projectWithTeams:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}
