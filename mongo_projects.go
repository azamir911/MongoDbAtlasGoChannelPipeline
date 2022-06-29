package main

import (
	"context"
	"github.com/rs/zerolog"
	"go.mongodb.org/atlas/mongodbatlas"
	"sync"
	"time"
)

func ProjectsStreamer(ctx context.Context, wg *sync.WaitGroup, client *mongodbatlas.Client) <-chan *mongodbatlas.Projects {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Projects, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Projects Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		// Declare the option to get only one project id
		options := &mongodbatlas.ListOptions{
			PageNum:      1,
			ItemsPerPage: 1,
			IncludeCount: false,
		}

		for {
			time.Sleep(time.Second)
			projects, _, err := client.Projects.GetAllProjects(ctx, options)
			if err != nil || projects == nil || len(projects.Results) == 0 {
				break
			}
			output <- projects
			options.PageNum++
		}
	}()
	return output
}

func ProjectsFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Projects) <-chan *mongodbatlas.Projects {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Projects, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Projects Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case projects, ok := <-input:
				if !ok {
					log.Debug().Msg("Projects Filter processing exist!")
					input = nil
					return
				} else if projects != nil && len(projects.Results) > 0 {
					log.Debug().Msg("Projects Filter processing working!")
					time.Sleep(time.Second)
					output <- projects
				}
			}
		}
	}()
	return output
}

func ProjectsMapper(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Projects) <-chan *mongodbatlas.Project {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Project, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Projects Mapper Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case projects, ok := <-input:
				if !ok {
					log.Debug().Msg("Projects Mapper processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Projects Mapper processing working!")
					time.Sleep(time.Second)
					for _, project := range projects.Results {
						output <- project
					}
				}
			}
		}
	}()
	return output
}

func ProjectFilter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Project) <-chan *mongodbatlas.Project {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	output := make(chan *mongodbatlas.Project, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Project Filter Closing channel output!")
			close(output)
			wg.Done()
		}()

		for {
			select {
			case project, ok := <-input:
				if !ok {
					log.Debug().Msg("Project Filter processing exist!")
					input = nil
					return
				} else if project != nil && project.ID != "" {
					log.Debug().Msg("Project Filter processing working!")
					time.Sleep(time.Second)
					output <- project
				}
			}
		}
	}()
	return output
}

func ProjectDuplicator(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Project) (<-chan *mongodbatlas.Project, <-chan *mongodbatlas.Project, <-chan *mongodbatlas.Project, <-chan *mongodbatlas.Project) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	outputA, outputB, outputC, outputD := make(chan *mongodbatlas.Project, 10), make(chan *mongodbatlas.Project, 10), make(chan *mongodbatlas.Project, 10), make(chan *mongodbatlas.Project, 10)

	go func() {
		defer func() {
			log.Debug().Msg("Project Duplicator Closing channel outputA and outputB!")
			close(outputA)
			close(outputB)
			close(outputC)
			close(outputD)
			wg.Done()
		}()

		for {
			select {
			case project, ok := <-input:
				if !ok {
					log.Debug().Msg("Project Duplicator processing exit!")
					input = nil
					return
				} else {
					log.Debug().Msg("Project Duplicator processing working!")
					time.Sleep(time.Second)
					outputA <- project
					outputB <- project
					outputC <- project
					outputD <- project
				}
			}
		}
	}()
	return outputA, outputB, outputC, outputD
}

func ProjectPrinter(ctx context.Context, wg *sync.WaitGroup, input <-chan *mongodbatlas.Project) {
	wg.Add(1)
	log := ctx.Value(cyLogger).(*zerolog.Logger)
	go func() {
		defer wg.Done()

		for {
			select {
			case project, ok := <-input:
				if !ok {
					log.Debug().Msg("Project Printer processing exist!")
					input = nil
					return
				}
				//time.Sleep(time.Second)
				log.Debug().Msg("Project Printer processing working!")
				log.Info().Msgf("\tProject: Id %v, Name %v, Created %v, Cluster %v", project.ID, project.Name, project.Created, project.ClusterCount)
			}
		}
	}()
}
