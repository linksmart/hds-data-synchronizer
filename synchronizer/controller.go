package synchronizer

import (
	"context"
	"log"
	"net/url"
	"time"

	"github.com/linksmart/historical-datastore/data"
)

type Controller struct {
	primaryHDS  data.GrpcClient
	mappingList *map[string]Replication
}

func NewController() *Controller {
	replicator := new(Controller)
	replicator.mappingList = new(map[string][]Destination)
	return replicator
}
func (c Controller) addToMappingList(series string, destinations []Destination) {
	if c.mappingList[series] == nil {
		c.primaryHDS.Subscribe(context.Background(), series)
	}
}
func (c Replicator) updateMappingList(series string, destinations []Destination) {

}

func (r Replicator) removeSeries(series string) {

}

func SubscribeAndPublish(srcEndpoint string, destEndpoint string, stream string) {
	srcUrl, err := url.Parse(srcEndpoint)
	if err != nil {
		log.Panicf("Error parsing source url: %v", err)
	}
	dstURL, err := url.Parse(destEndpoint)
	if err != nil {
		log.Panicf("Error parsing dest url: %v", err)
	}
	src, err := data.NewGrpcClient(srcUrl.Host) //
	if err != nil {
		log.Panic(err)
	}
	total, err := src.Count([]string{stream}, data.Query{To: time.Now(), From: time.Now().Add(-time.Hour * 24)})
	if err != nil {
		log.Printf("Error getting total: %v", err)
	}

	log.Println(total)
	dst, err := data.NewGrpcClient(dstURL.Host) //
	if err != nil {
		log.Panic(err)
	}
	totalDest, err := dst.Count([]string{stream}, data.Query{To: time.Now(), From: time.Now().Add(-time.Hour * 24)})
	if err != nil {
		log.Printf("Error getting total: %v", err)
	}
	log.Println(totalDest)

}
