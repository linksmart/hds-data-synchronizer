package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/linksmart/hds-data-synchronizer/common"
	sync "github.com/linksmart/hds-data-synchronizer/synchronizer"
)

type ThingDescriptionPage struct {
	Context string             `json:"@context,omitempty"`
	Items   []ThingDescription `json:"items"`
	Page    int                `json:"page"`
	PerPage int                `json:"perPage"`
	Total   int                `json:"total"`
}

type ThingDescription = map[string]interface{}

func main() {
	// a map containing the array of destination replica nodes for each series name
	//seriesMap := make(map[string][]string)
	var (
		confPath = flag.String("conf", "conf/conf.json", "HDS Sync configuration file path")
	)
	flag.Parse()

	conf, err := common.LoadConfig(confPath)
	if err != nil {
		log.Panicf("Cannot load configuration: %v", err)
	}

	log.Println("starting synchronization")
	syncController, err := sync.NewController(conf)
	if err != nil {
		log.Panicf("Error initializing synchronization: %s", err)
	}
	err = syncController.StartSyncForAll()
	if err != nil {
		log.Panicf("Error starting synchronization: %s", err)
	}

	handler := make(chan os.Signal, 1)
	// Ctrl+C / Kill handling
	signal.Notify(handler, os.Interrupt, os.Kill)
	<-handler
	log.Println("Shutting down...")
	syncController.StopSyncForAll()
	log.Println("Stopped.")

}
