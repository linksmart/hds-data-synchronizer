package sync

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/linksmart/hds-data-synchronizer/common"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type Controller struct {
	// SyncMap contains the map of series with active synchronization
	SyncMap map[string]*Synchronizer

	// srcDataClient is the connection to the source host
	srcDataClient *data.GrpcClient
	// dstDataClient is the connection to the destination host
	dstDataClient *data.GrpcClient
	// srcRegistryClient
	srcRegistryClient *registry.GrpcClient
	// dstRegistryClient
	dstRegistryClient *registry.GrpcClient

	syncInterval   time.Duration
	destinationURL string
	sourceURL      string

	//stopSync is set when the sync application stops
	stopSync chan bool
}

func NewController(conf *common.Config) (*Controller, error) {
	controller := new(Controller)
	var err error
	controller.destinationURL = conf.Destination
	controller.sourceURL = conf.Source
	controller.syncInterval, err = time.ParseDuration(conf.SyncInterval)
	if err != nil {
		return nil, fmt.Errorf("unable to parse synchronization interval:%w", err)
	}

	// get the clients for source
	controller.srcRegistryClient, controller.srcDataClient, err = getClients(conf, conf.Source)
	if err != nil {
		return nil, fmt.Errorf("error initializing  gRPC client for source %s: %w", conf.Source, err)
	}

	// get the clients for source
	controller.dstRegistryClient, controller.dstDataClient, err = getClients(conf, conf.Destination)
	if err != nil {
		return nil, fmt.Errorf("error initializing  gRPC client for destination %s: %w", conf.Destination, err)
	}

	controller.SyncMap = make(map[string]*Synchronizer)
	controller.stopSync = make(chan bool)
	return controller, nil
}

func getClients(conf *common.Config, urlStr string) (*registry.GrpcClient, *data.GrpcClient, error) {
	serverCertFile := conf.TLS.Cert
	serverPrivatekey := conf.TLS.Key
	caFile := conf.TLS.CA
	// Load the certificates from disk
	certificate, err := tls.LoadX509KeyPair(serverCertFile, serverPrivatekey)
	if err != nil {
		return nil, nil, fmt.Errorf("could not load client key pair: %s", err)
	}

	// Create a certificate pool from the certificate authority
	// Get the SystemCertPool, continue with an empty pool on error
	certPool, _ := x509.SystemCertPool()
	if certPool == nil {
		certPool = x509.NewCertPool()
	}
	ca, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read CA certificate: %s", err)
	}

	// Append the client certificates from the CA
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, nil, fmt.Errorf("failed to append client certs")
	}

	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse url %s: %s", conf.Destination, err)
	}

	hostPort := parsedUrl.Host
	var host string
	if strings.Contains(hostPort, ":") {
		host, _, err = net.SplitHostPort(hostPort)
		if err != nil {
			return nil, nil, fmt.Errorf("error splitting the port and host name from %s: %v", urlStr, err)
		}
	} else {
		host = hostPort
	}
	// get the transport credentials
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   host,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})

	// Connect to the destination
	dataClient, err := data.NewGrpcClient(hostPort, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("error dialing to the destination: %w", err)
	}
	registryClient, err := registry.NewGrpcClient(hostPort, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, fmt.Errorf("error dialing to the destination: %w", err)
	}
	return registryClient, dataClient, nil
}

func (c Controller) StartSyncForAll() {

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		err := c.updateSyncing()
		if err != nil {
			log.Println(err)
		}
		for {
			select {
			case <-c.stopSync:
				ticker.Stop()
				return
			case <-ticker.C:
				err := c.updateSyncing()
				if err != nil {
					log.Println(err)
				}
			}
		}
		defer ticker.Stop()
	}()

}
func (c Controller) updateSyncing() error {
	// Get all the registry entries
	page := 1
	perPage := 100
	remaining := 0
	skipDelete := make(map[string]bool)
	log.Printf("Fetching registry of %s", c.sourceURL)
	for do := true; do; do = remaining > 0 {

		seriesList, total, err := c.srcRegistryClient.GetMany(page, perPage)
		if err != nil {
			return fmt.Errorf("error fetching registry of %s:%v", c.sourceURL, err)
		}
		// For each registry entry, check if the synchronization is enabled for that particular time series
		if page == 1 {
			remaining = total
		}
		remaining = remaining - len(seriesList)
	seriesLoop:
		for _, series := range seriesList {
			skipDelete[series.Name] = true
			if _, ok := c.SyncMap[series.Name]; ok {
				// the series is being synced already. continue to other series
				continue
			}
			err = c.dstRegistryClient.Add(series)
			if err != nil {
				if st, ok := status.FromError(err); ok {
					if st.Code() != codes.AlreadyExists {
						log.Printf("error creating registry in destination:%s:%v", series.Name, err)
						continue seriesLoop
					} else {
						log.Printf("Continuing with existing timeseries %s in destination", series.Name)
					}
				}
			} else {
				log.Printf("Created timeseries %s in destination", series.Name)
			}
			time.Sleep(time.Second)
			c.SyncMap[series.Name] = newSynchronization(series.Name, c.srcDataClient, c.dstDataClient, c.syncInterval)
		}
		page += 1
	}

	for seriesName, series := range c.SyncMap {
		if _, ok := skipDelete[seriesName]; !ok {
			series.clear()
		}
	}
	return nil
}
func (c Controller) StopSyncForAll() {
	c.stopSync <- true
	for _, s := range c.SyncMap {
		s.clear()
	}
}
