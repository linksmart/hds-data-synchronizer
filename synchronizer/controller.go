package synchronizer

import (
	"crypto/tls"
	"fmt"
	"log"

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

func NewController(primaryHDSHost string, srcHDSCA string, cd *certs.CertDirectory) (*Controller, error) {
	controller := new(Controller)
	controller.cd = cd
	creds, err := getCreds(cd, primaryHDSHost, srcHDSCA)
	if err != nil {
		return nil, err
	}
	hds, err := data.NewGrpcClient(primaryHDSHost, grpc.WithTransportCredentials(*creds)) //
	if err != nil {
		return nil, err
	}
	log.Printf("connected to source: %s", primaryHDSHost)
	controller.primaryHDS = hds
	controller.mappingList = map[string]*Synchronization{}
	controller.destConnMap = map[string]*data.GrpcClient{}
	return controller, nil
}

func (c *Controller) AddOrUpdateSeries(series string, destinationHosts []string, caEndpoints []string) {
	destConns := map[string]*data.GrpcClient{}
	for i, destHost := range destinationHosts {
		if c.destConnMap[destHost] == nil {
			creds, err := getCreds(c.cd, destHost, caEndpoints[i])
			if err != nil {
				log.Printf("unable to setup TLS: host:%s, err:%v", destHost, err)
				continue
			}
			conn, err := data.NewGrpcClient(destHost, grpc.WithTransportCredentials(*creds)) //
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

func getCreds(cd *certs.CertDirectory, addr string, caEndpoint string) (*credentials.TransportCredentials, error) {
	cert, err := cd.GetCert(addr, caEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error in GetCert: %v", err)
	}
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

	creds := credentials.NewTLS(&tls.Config{
		ServerName:   addr, // NOTE: this is required!
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return &creds, nil
}
