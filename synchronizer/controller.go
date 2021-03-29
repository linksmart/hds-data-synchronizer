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

	SyncMap map[string]*Synchronization
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

	// get the transport credentials
	creds, err := getClientTransportCredentials(conf)
	if err != nil {
		return nil, fmt.Errorf("error getting transport credentials: %w", err)
	}

	srcUrl, err := url.Parse(conf.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s: %s", conf.Destination, err)
	}
	src := srcUrl.Host
	dstUrl, err := url.Parse(conf.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s: %s", conf.Destination, err)
	}
	dst := dstUrl.Host
	// Connect to the source
	controller.srcDataClient, err = data.NewGrpcClient(src, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, fmt.Errorf("error dialing to the destination: %w", err)
	}
	controller.srcRegistryClient, err = registry.NewGrpcClient(src, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, fmt.Errorf("error dialing to the destination: %w", err)
	}

	// Connect to the destination
	controller.dstDataClient, err = data.NewGrpcClient(dst, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, fmt.Errorf("error dialing to the destination: %w", err)
	}
	controller.dstRegistryClient, err = registry.NewGrpcClient(dst, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, fmt.Errorf("error dialing to the destination: %w", err)
	}
	controller.SyncMap = make(map[string]*Synchronization)
	return controller, nil
}

func getClientTransportCredentials(conf *common.Config) (*credentials.TransportCredentials, error) {
	serverCertFile := conf.TLS.Cert
	serverPrivatekey := conf.TLS.Key
	caFile := conf.TLS.CA
	// Load the certificates from disk
	certificate, err := tls.LoadX509KeyPair(serverCertFile, serverPrivatekey)
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %s", err)
	}

	// Create a certificate pool from the certificate authority
	// Get the SystemCertPool, continue with an empty pool on error
	certPool, _ := x509.SystemCertPool()
	if certPool == nil {
		certPool = x509.NewCertPool()
	}
	ca, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	// Append the client certificates from the CA
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, fmt.Errorf("failed to append client certs")
	}
	url, err := url.Parse(conf.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s: %s", conf.Destination, err)
	}
	host := url.Host
	if strings.Contains(host, ":") {
		host, _, err = net.SplitHostPort(host)
		if err != nil {
			return nil, fmt.Errorf("error splitting the port and host name from %s: %v", conf.Destination, err)
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   host,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return &creds, nil
}

func (c Controller) StartSyncForAll() error {
	// Get all the registry entries
	page := 1
	perPage := 100
	remaining := 0
	for do := true; do; do = remaining > 0 {

		seriesList, total, err := c.srcRegistryClient.GetMany(page, perPage)
		if err != nil {
			return fmt.Errorf("error getting registry:%v", err)
		}
		// For each registry entry, check if the synchronization is enabled for that particular time series
		if page == 1 {
			remaining = total
		}
		remaining = remaining - len(seriesList)

		for _, series := range seriesList {
			err = c.dstRegistryClient.Add(series)
			if err != nil {
				if st, ok := status.FromError(err); ok {
					if st.Code() != codes.AlreadyExists {
						return fmt.Errorf("error for creating registry in destination:%s:%v", series.Name, err)
					} else {
						log.Printf("Continuing with existing timeseries %s in destination", series.Name)
					}
				}
			} else {
				log.Printf("Created timeseries %s in destination", series.Name)
			}
			c.SyncMap[series.Name] = newSynchronization(series.Name, c.srcDataClient, c.dstDataClient, c.syncInterval)
		}
		page += 1
	}
	return nil
}

func (c Controller) UpdateSync(series string) {
	//TODO:
	// Check if the synchronization is enabled or not.
	// If disabled, disable the enabled thread
	// If enabled, enable the disabled thread
}

func (c Controller) StopSyncForAll() {
	for _, s := range c.SyncMap {
		s.clear()
	}
}
