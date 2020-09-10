package synchronizer

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/linksmart/historical-datastore/data"
)

type Synchronization struct {
	series        string
	// sourceClient is the connection to the source host
	sourceClient  *data.GrpcClient
	// sourceChannel is  channel for the subscribed series
	sourceChannel chan data.ResponsePack
	// destinations is the map of destination host to connections
	destinations  map[string]Destination

	// latest is the time corresponding to the latest record in the source
	latest time.Time
}

type Destination struct {
	lastTS          time.Time
	firstTS         time.Time
	client          *data.GrpcClient
	runningFallback bool
	fallbackMutex   *sync.Mutex
}

func newSynchronization(series string, sourceClient *data.GrpcClient, destinations map[string]*data.GrpcClient) (s *Synchronization) {
	s = &Synchronization{
		series:       series,
		sourceClient: sourceClient,
		destinations: map[string]Destination{},
	}

	s.addDestinations(destinations)
	go s.replicate()
	return s
}

func (s Synchronization) replicate() {
	var err error
	s.sourceChannel, err = s.sourceClient.Subscribe(context.Background(), s.series)
	log.Printf("Success subscribing to source")
	if err != nil {
		log.Printf("error subscribing:%v", err)
	}

	for response := range s.sourceChannel {
		if response.Err != nil {
			log.Printf("error while recieving stream: %v", response.Err)
		}
		pack := response.Pack
		latestTs := data.FromSenmlTime(pack[len(pack)-1].Time)
		for key, dest := range s.destinations {
			if dest.lastTS.Equal(latestTs) == false {
				go s.fallback(key,dest.lastTS,latestTs)
			}
			log.Printf("copying %d entries to replica %s", len(pack), key)
			dest.client.Submit(pack)
		}
	}

}

func (s Synchronization) addDestinations(destinations map[string]*data.GrpcClient) {
	for k, v := range destinations {
		//go syncOldData(dest, 0)
		destination := Destination{client: v, lastTS: time.Time{},firstTS: time.Time{},runningFallback: false, fallbackMutex: &sync.Mutex{}}

		s.destinations[k] = destination
		go s.fallback(k,destination.firstTS,time.Now())
	}
}
func (s Synchronization) removeDestinations(conns []string) {
	for _,conn := range conns {
		delete(s.destinations, conn)
	}
}

func (s Synchronization) updateDestionations(destinationHosts map[string]*data.GrpcClient)  {
	var  toDelete []string
	toAdd :=  map[string]*data.GrpcClient{}
	for k,v := range destinationHosts {
		if s.destinations[k] == nil {
			toAdd[k] = v
		}
	}

	for k,_ := range s.destinations {
		if destinationHosts[k] == nil {
			toDelete = append(toDelete,k)
		}
	}

	s.addDestinations(toAdd)
	s.removeDestinations(toDelete)
}

func (s Synchronization) fallback(dest string, from time.Time, to time.Time) {
	destination := s.destinations[dest]

	//fallback is supposed to run only once
	destination.fallbackMutex.Lock()
	if destination.runningFallback {
		destination.fallbackMutex.Unlock()
		return
	}
	destination.runningFallback = true
	destination.fallbackMutex.Unlock()


	defer func() {
		destination.fallbackMutex.Lock()
		destination.runningFallback = false
		destination.fallbackMutex.Unlock()
	}()

	lastTimeStamp,err := getLastTime(destination.client, s.series,from,to)
	if err != nil {
		log.Printf("Error getting the last timestamp: %v",err)
		return
	}
	if to.Equal(lastTimeStamp) {
		log.Printf("Skipping fallback as the destination %s is already updated for stream %s",dest, s.series)
	}else {
		log.Printf("Starting fallback for destination :%s, series: %s",dest, s.series)
	}
	destStream, err := destination.client.CreateSubmitStream(context.Background())
	if err != nil {
		log.Printf("Error getting the stream: %v",err)
		return
	}

	defer destination.client.CloseSubmitStream(destStream)

	sourceChannel, err := s.sourceClient.QueryStream(context.Background(),[]string{s.series},data.Query{From: from, To: to,SortAsc: true})
	if err != nil {
		log.Printf("Error querying the source: %v",err)
		return
	}
	for response := range sourceChannel {
		if response.Err != nil {
			log.Printf("Breaking as there was error while recieving stream: %v", response.Err)
			break
		}
		err = destination.client.SubmitToStream(destStream,response.Pack)
		if err != nil {
			log.Printf("Breaking as there was error while recieving stream: %v", err)
			break
		}
	}
}

func getLastTime(client *data.GrpcClient, series string, from time.Time,to time.Time) (time.Time,error) {
	pack, err := client.Query([]string{series},data.Query{From:from,To: to,Limit: 1,SortAsc: false})
	if err != nil {
		return time.Time{},err
	}
	if len(pack) != 1 {
		return to,nil
	}
	return data.FromSenmlTime(pack[0].Time),err
}

