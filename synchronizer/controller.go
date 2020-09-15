package synchronizer

import (
	"log"

	"github.com/linksmart/historical-datastore/data"
)

var info log.Logger
var debug log.Logger

type Controller struct {
	primaryHDS  *data.GrpcClient
	mappingList map[string]*Synchronization
	destConnMap map[string]*data.GrpcClient
}

func NewController(primaryHDSHost string) (*Controller, error) {
	controller := new(Controller)
	hds, err := data.NewGrpcClient(primaryHDSHost) //
	if err != nil {
		return nil, err
	}
	controller.primaryHDS = hds
	controller.mappingList = map[string]*Synchronization{}
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
				continue //ToDo: A retry atempt for failed nodes
			}
			log.Printf("Connected to destionation host: %s", destHost)
			c.destConnMap[destHost] = conn
		}
		destConns[destHost] = c.destConnMap[destHost]
	}
	if c.mappingList[series] == nil {
		r := newSynchronization(series, c.primaryHDS, destConns)
		c.mappingList[series] = r
	} else {
		s := c.mappingList[series]
		s.updateDestionations(destConns)
	}
}

func (c *Controller) removeSeries(series string) {
	c.mappingList[series].clear()
}
