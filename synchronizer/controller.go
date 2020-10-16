package synchronizer

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"

	"github.com/linksmart/hds-data-synchronizer/certs"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/pki"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var info log.Logger
var debug log.Logger

type Controller struct {
	primaryHDS  *data.GrpcClient
	mappingList map[string]*Synchronization //series to destination map
	destConnMap map[string]*data.GrpcClient //destination to connection map
	cd          *certs.CertDirectory
}

func NewController(primaryHDSHost string, cd *certs.CertDirectory) (*Controller, error) {
	controller := new(Controller)
	controller.cd = cd
	creds, err := getCreds(cd, primaryHDSHost)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the transport credentials for %s: %v", primaryHDSHost, err)
	}
	hostUrl, err := url.Parse(primaryHDSHost)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s: %v", primaryHDSHost, err)
	}
	hds, err := data.NewGrpcClient(hostUrl.Host+hostUrl.Path, grpc.WithTransportCredentials(*creds)) //
	if err != nil {
		return nil, fmt.Errorf("unable to connect to %s: %v", primaryHDSHost, err)
	}
	log.Printf("connected to source: %s", primaryHDSHost)
	controller.primaryHDS = hds
	controller.mappingList = map[string]*Synchronization{}
	controller.destConnMap = map[string]*data.GrpcClient{}
	return controller, nil
}

func (c *Controller) AddOrUpdateSeries(series string, destinationHosts []string) {
	destConns := map[string]*data.GrpcClient{}
	for _, destHost := range destinationHosts {
		if c.destConnMap[destHost] == nil {
			creds, err := getCreds(c.cd, destHost)
			if err != nil {
				log.Printf("unable to setup TLS: host:%s, err:%v", destHost, err)
				continue
			}
			hostUrl, err := url.Parse(destHost)
			if err != nil {
				log.Printf("failed to parse %s: %v", destHost, err)
				continue //ToDo: A retry attempt for failed nodes
			}
			conn, err := data.NewGrpcClient(hostUrl.Host+hostUrl.Path, grpc.WithTransportCredentials(*creds)) //
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

func getCreds(cd *certs.CertDirectory, addr string) (*credentials.TransportCredentials, error) {
	cert := cd.Cert
	certPEM, err := pki.CertificateToPEM(*cert)
	if err != nil {
		return nil, fmt.Errorf("error converting cert to PEM: %v", err)
	}
	key := cd.Key
	keyPEM, err := pki.PrivateKeyToPEM(key)
	if err != nil {
		return nil, fmt.Errorf("error converting key to PEM: %v", err)
	}

	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	certPool := cd.CAPool
	serverUrl, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("error parsing the url %s: %v", addr, err)
	}
	host := serverUrl.Host
	if strings.Contains(host, ":") {
		host, _, err = net.SplitHostPort(serverUrl.Host)
		if err != nil {
			return nil, fmt.Errorf("error splitting the port and host name from %s: %v", addr, err)
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   host,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return &creds, nil
}
