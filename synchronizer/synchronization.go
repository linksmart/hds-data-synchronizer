package synchronizer

import (
	"context"
	"log"

	"github.com/linksmart/historical-datastore/data"
)

type Replication struct {
	series        string
	sourceClient  *data.GrpcClient
	sourceChannel chan data.ResponsePack
	destinations  map[string]Destination
}

type Destination struct {
	lastTS float64
	client *data.GrpcClient
}

func newReplication(series string, sourceClient *data.GrpcClient, destinations map[string]*data.GrpcClient) (r *Replication) {
	r = &Replication{
		series:       series,
		sourceClient: sourceClient,
		destinations: map[string]Destination{},
	}
	for k, v := range destinations {
		//go syncOldData(dest, 0)
		destination := Destination{client: v, lastTS: 0}

		r.destinations[k] = destination
	}

	go r.replicate()
	return r
}

func (r Replication) replicate() {
	var err error
	r.sourceChannel, err = r.sourceClient.Subscribe(context.Background(), r.series)
	log.Printf("Success subscribing to source")
	if err != nil {
		log.Printf("error subscribing:%v", err)
	}
	for response := range r.sourceChannel {
		if response.Err != nil {
			log.Printf("error while recieving stream: %v", response.Err)
		}
		pack := response.Pack
		for key, dest := range r.destinations {
			log.Printf("copying %d entries to replica %s", len(pack), key)
			dest.client.Submit(pack)
		}
	}

}

func (r Replication) updateDestinations(conns map[string]*data.GrpcClient) {
	log.Panic("implement me")
}
