package synchronizer

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
)

type Synchronization struct {
	series string
	// sourceClient is the connection to the source host
	sourceClient *data.GrpcClient
	// destinations is the map of destination host to connections
	destinations map[string]*Destination
	// latest is the time corresponding to the latest record in the source
	latest time.Time
	// ctx is the context passed to gRPC Calls
	ctx context.Context
	// cancel function to cancel any of the running gRPC communication whenever the destination needs to be deleted
	cancel context.CancelFunc
}

type fallbackThread struct {
	// running is set to true if the fallback is running
	running bool
	// mutex to protect the "running" variable
	mutex *sync.Mutex
}

type Destination struct {
	// lastTS holds the latest measurement present in the destination HDS
	lastTS time.Time
	// firstTS holds the starting time from which sync needs to start
	firstTS time.Time
	// client holds the gRPC client related to the destination
	client *data.GrpcClient
	// fallback holds the information related to the fallback thread
	fallback fallbackThread
	// ctx holds the context to be passed to gRPC calls
	ctx context.Context
	// cancel function to cancel any of the running gRPC communication whenever the destination needs to be deleted
	cancel context.CancelFunc
}

func newSynchronization(series string, sourceClient *data.GrpcClient, destinations map[string]*data.GrpcClient) (s *Synchronization) {
	s = &Synchronization{
		series:       series,
		sourceClient: sourceClient,
		destinations: map[string]*Destination{},
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.addDestinations(destinations)
	go s.synchronize()
	return s
}

// clear ensures graceful shutdown of the synchronization related to the series
func (s Synchronization) clear() {
	s.cancel()
	for k, dest := range s.destinations {
		dest.cancel()
		delete(s.destinations, k)
	}
}

func (s Synchronization) synchronize() {
	for true {
		s.subscribeAndPublish()
		time.Sleep(time.Second)
		log.Printf("retrying subscription")
	}

}

func (s Synchronization) subscribeAndPublish() {
	// get the latest measurement from source
	var err error
	s.latest, err = getLastTime(s.sourceClient, s.series, time.Time{}, time.Now())
	if err != nil {
		log.Printf("error getting latest measurement:%v", err)
		return
	}

	//subscribe to source HDS
	responseCh, err := s.sourceClient.Subscribe(s.ctx, s.series)
	log.Printf("Success subscribing to source")
	if err != nil {
		log.Printf("error subscribing:%v", err)
		return
	}

	for response := range responseCh {
		if response.Err != nil {
			log.Printf("error while recieving stream: %v", response.Err)
			return
		}
		pack := response.Pack
		latestInPack := getLatestInPack(pack)
		for key, dest := range s.destinations {
			if dest.lastTS.Equal(s.latest) == false {
				log.Printf("src and destination time (%v vs %v) do not match. starting fallback until %v", s.latest, dest.lastTS, latestInPack)
				go s.fallback(key, dest.lastTS, latestInPack)
				continue
			}
			log.Printf("copying %d entries to replica %s", len(pack), key)
			err = dest.client.Submit(dest.ctx, pack)
			if err != nil {
				log.Printf("Error copying entries : %v", err)
			} else {
				dest.lastTS = latestInPack
			}
		}

		s.latest = latestInPack

	}

}

func getLatestInPack(pack senml.Pack) time.Time {
	//Since it is not assured that the pack will be sorted, we search exhaustively to find the latest
	latestInPack := pack[0].Time
	for _, r := range pack {
		if r.Time > latestInPack {
			latestInPack = r.Time
		}
	}
	return data.FromSenmlTime(latestInPack)
}

func (s Synchronization) addDestinations(destinations map[string]*data.GrpcClient) {
	for k, v := range destinations {
		//go syncOldData(dest, 0)
		destination := Destination{
			client:  v,
			lastTS:  time.Time{},
			firstTS: time.Time{},
			fallback: fallbackThread{
				running: false,
				mutex:   &sync.Mutex{},
			},
		}

		destination.ctx, destination.cancel = context.WithCancel(context.Background())

		s.destinations[k] = &destination
		go s.fallback(k, destination.firstTS, time.Now())
	}
}
func (s Synchronization) removeDestinations(conns []string) {
	for _, conn := range conns {
		s.destinations[conn].cancel()
		delete(s.destinations, conn)
	}
}

func (s Synchronization) updateDestionations(destinationHosts map[string]*data.GrpcClient) {
	var toDelete []string
	toAdd := map[string]*data.GrpcClient{}
	for k, v := range destinationHosts {
		if s.destinations[k] == nil {
			toAdd[k] = v
		}
	}

	for k, _ := range s.destinations {
		if destinationHosts[k] == nil {
			toDelete = append(toDelete, k)
		}
	}

	s.addDestinations(toAdd)
	s.removeDestinations(toDelete)
}

func (s Synchronization) fallback(dest string, from time.Time, to time.Time) {
	destination := s.destinations[dest]

	//fallback is supposed to run only once
	destination.fallback.mutex.Lock()
	if destination.fallback.running {
		destination.fallback.mutex.Unlock()
		return
	}
	destination.fallback.running = true
	destination.fallback.mutex.Unlock()

	defer func() {
		destination.fallback.mutex.Lock()
		destination.fallback.running = false
		destination.fallback.mutex.Unlock()
	}()

	destLatest, err := getLastTime(destination.client, s.series, from, to)
	if err != nil {
		log.Printf("Error getting the last timestamp: %v", err)
		return
	}
	log.Printf("Last timestamp: %s", destLatest)

	if to.Equal(destLatest) {
		log.Printf("Skipping fallback as the destination %s is already updated for stream %s", dest, s.series)
	} else if to.Before(destLatest) {
		log.Println("destination is ahead of source. Should not have happened!!")
	} else {
		log.Printf("Starting fallback for destination :%s, series: %s, dest latest: %v, to:%v", dest, s.series, destLatest, to)
	}
	ctx := destination.ctx
	destStream, err := destination.client.CreateSubmitStream(ctx)
	if err != nil {
		log.Printf("Error getting the stream: %v", err)
		return
	}

	defer destination.client.CloseSubmitStream(destStream)

	sourceChannel, err := s.sourceClient.QueryStream(ctx, []string{s.series}, data.Query{From: destLatest, To: to.Add(time.Second), SortAsc: true})
	if err != nil {
		log.Printf("Error querying the source: %v", err)
		return
	}
	for response := range sourceChannel {
		if response.Err != nil {
			log.Printf("Breaking as there was error while recieving stream: %v", response.Err)
			break
		}
		err = destination.client.SubmitToStream(destStream, response.Pack)
		if err != nil {
			log.Printf("Breaking as there was error while submitting stream: %v", err)
			break
		}
		destination.lastTS = getLatestInPack(response.Pack)

	}
	log.Printf("done with fallback. destination latest: %v", destination.lastTS)

}

func getLastTime(client *data.GrpcClient, series string, from time.Time, to time.Time) (time.Time, error) {
	pack, err := client.Query(context.Background(), []string{series}, data.Query{From: from, To: to, Limit: 1, SortAsc: false})
	if err != nil {
		return time.Time{}, err
	}
	if len(pack) != 1 {
		return to, nil
	}
	return data.FromSenmlTime(pack[0].Time), err
}
