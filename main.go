package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/hds-data-synchronizer/certs"
	"github.com/linksmart/hds-data-synchronizer/common"
	"github.com/linksmart/hds-data-synchronizer/synchronizer"
	"github.com/linksmart/hds-data-synchronizer/utils"
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

	conf, err := loadConfig(confPath)
	if err != nil {
		log.Panicf("Cannot load configuration: %v", err)
	}
	var store certs.Store
	var closeCertStore func() error

	cd, err := certs.NewCertDirectory(conf.TLS, store)
	if err != nil {
		log.Panicf("Cannot initiate certificate directory: %v", err)
	}

	controller, err := synchronizer.NewController(conf.HDS, cd)

	if err != nil {
		log.Panicf("Cannot connect to source hds: %v", err)
	}

	ticker := time.NewTicker(1 * time.Minute)

	handler := make(chan os.Signal, 1)
	// Ctrl+C / Kill handling
	signal.Notify(handler, os.Interrupt, os.Kill)

	defer ticker.Stop()

	TDDWatcher(conf.HDS, conf.TDDConf, controller)

TDDWatchLoop:
	for true {
		select {
		case <-ticker.C:
			TDDWatcher(conf.HDS, conf.TDDConf, controller)
		case <-handler:
			log.Println("breaking the TDD watcher loop")
			break TDDWatchLoop
		}
	}

	log.Println("Shutting down...")

	if closeCertStore != nil {
		err := closeCertStore()
		if err != nil {
			log.Println(err.Error())
		}
	}

}

func TDDWatcher(primaryHDS string, tddConf common.TDDConf, c *synchronizer.Controller) {
	tddEndpoint := strings.TrimLeft(tddConf.Endpoint, "/") + "/td"
	log.Printf("Updating the list based on TD : %s", tddEndpoint)

	query := "xpath=*[primaryHDS='" + primaryHDS + "']"
	urlStr := tddEndpoint + "?" + query
	var ticket *obtainer.Client
	var err error
	if tddConf.Auth.Enabled {
		ticket, err = obtainer.NewClient(tddConf.Auth.Provider, tddConf.Auth.ProviderURL, tddConf.Auth.Username, tddConf.Auth.Password, tddConf.Auth.ClientID)
		if err != nil {
			log.Printf("error creating auth client: %s", err)
			return
		}
	}

	res, err := utils.HTTPRequest("GET",
		urlStr,
		nil, nil, ticket)

	if err != nil {
		log.Printf("requesting TD failed: %v", err)
		return
	}

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		log.Printf("unexpected status %d", res.StatusCode)
		return
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf("unexpected error while decoding the body %v", err)
		return
	}
	var tdPage ThingDescriptionPage
	err = json.Unmarshal(body, &tdPage)
	if err != nil {
		log.Printf("unexpected error while decoding the body %v", err)
		return
	}
	for _, td := range tdPage.Items {
		seriesName := td["series"].(string) //strings.TrimLeft(td["id"].(string),primaryHDS+"/data/")
		links := td["links"]
		linkArr := links.([]interface{})
		var destHosts []string
		for _, val := range linkArr {
			link := val.(map[string]interface{})
			if link["rel"].(string) != "replica" {
				continue
			}
			href := link["href"].(string)
			destHosts = append(destHosts, href)
		}
		c.AddOrUpdateSeries(seriesName, destHosts)
	}
}
