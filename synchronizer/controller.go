package synchronizer

import (
	"log"
	"net/url"
	"time"

	"github.com/linksmart/historical-datastore/data"
)

type Controller struct {
	primaryHDS  *data.GrpcClient
	mappingList map[string]*Replication
	destConnMap map[string]*data.GrpcClient
}

func NewController(primaryHDSHost string) (*Controller, error) {
	controller := new(Controller)
	hds, err := data.NewGrpcClient(primaryHDSHost) //
	if err != nil {
		return nil, err
	}
	controller.primaryHDS = hds
	controller.mappingList = map[string]*Replication{}
	controller.destConnMap = map[string]*data.GrpcClient{}
	return controller, nil
}

func (c *Controller) AddOrUpdateSeries(series string, destinationHosts []string) {
	destConns := map[string]*data.GrpcClient{}
	for _, destHost := range destinationHosts {
		if c.destConnMap[destHost] == nil {
			conn, err := data.NewGrpcClient(destHost) //
			if err != nil {
				log.Printf("unable to connect to %s: %v", destHost, err)
				continue
			}
			log.Printf("Connected to destionation host: %s", destHost)
			c.destConnMap[destHost] = conn
		}
		destConns[destHost] = c.destConnMap[destHost]
	}
	if c.mappingList[series] == nil {
		r := newReplication(series, c.primaryHDS, destConns)
		c.mappingList[series] = r
	} else {
		r := c.mappingList[series]
		r.updateDestinations(destConns)
	}
}

func (c *Controller) removeSeries(series string) {

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
