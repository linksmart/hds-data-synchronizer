package synchronizer

import (
	"context"
	"fmt"
	"log"

	"github.com/linksmart/historical-datastore/data"
)

type Replication struct {
	series        string
	sourceClient  GrpcClient
	sourceChannel chan ResponsePack
	destinations  []Destination
}

type Destination struct {
	lastTS float64
	client data.GrpcClient
}

func newReplication(series string, destinations []GrpcClient, sourceClient GrpcClient) (r *Replication, err error) {
	r = &Replication{
		series:       series,
		sourceClient: sourceClient,
	}
	for _, v := range destinations {
		//go syncOldData(dest, 0)
		destination := Destination{client: v, lastTS: 0}

		r.destinations = append(r.destinations, destination)
	}

	r.sourceChannel, err = sourceClient.Subscribe(context.Background(), series)
	if err != nil {
		return nil, fmt.Errorf("error subscribing:%w", err)
	}
	go r.replicate()
	return r, nil
}

func (r Replication) replicate() {
	for response := range r.sourceChannel {
		if response.err != nil {
			log.Printf("error while recieving stream: %v", response.err)
		}
		pack := response.p
		for _, dest := range r.destinations {
			dest.client.Submit(pack)
		}
	}

}
