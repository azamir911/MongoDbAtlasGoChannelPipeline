package elastic

import (
	connector2 "MongoDbAtlasGoChannelPipeline/integrations/elastic/connector"
	"context"
	"github.com/rs/zerolog"
	"sync"
)

func accountStreamer(ctx context.Context, wg *sync.WaitGroup, connector connector2.ElasticCloudConnector) <-chan string {
	wg.Add(1)
	log := ctx.Value(CyLogger).(*zerolog.Logger)
	output := make(chan string, 1)

	go func() {
		defer func() {
			log.Debug().Msg("Account Streamer Closing channel output!")
			close(output)
			wg.Done()
		}()

		//time.Sleep(time.Second)
		account, err := connector.GetAccount(ctx)
		if err != nil {
			log.Err(err).Msg("Failed to get account")
			return
		}
		if account == nil || account.Id == "" {
			return
		}

		select {
		case output <- account.Id:
		case <-ctx.Done():
			return
		}
	}()

	return output
}
