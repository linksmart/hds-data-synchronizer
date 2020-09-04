package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/linksmart/hds-data-synchronizer/synchronizer"
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
	tddUrl := "http://localhost:8081/td"

	primaryHDS := "localhost:8088"

	controller, err := synchronizer.NewController(primaryHDS)

	if err != nil {
		log.Panic("cannot connect to source hds")
	}
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	go func() {
		for ; true; <-ticker.C {
			TDDWatcher(primaryHDS, tddUrl, controller)
			break
		}
	}()
	// Ctrl+C / Kill handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)

	<-handler
	log.Println("stopping...")

}

func TDDWatcher(primaryHDS, tddUrl string, c *synchronizer.Controller) {

	query := "xpath=*[primaryHDS='" + primaryHDS + "']"

	res, err := http.Get(tddUrl + "?" + query)

	if err != nil {
		log.Printf("requesting TD failed!!")
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
			dstURL, err := url.Parse(href)
			if err != nil {
				log.Panicf("Error parsing dest url: %v", err)
			}
			destHosts = append(destHosts, dstURL.Host)
		}
		c.AddOrUpdateSeries(seriesName, destHosts)
	}
}
