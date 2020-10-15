package certs

import (
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/linksmart/hds-data-synchronizer/common"
	"github.com/linksmart/historical-datastore/pki"
)

type CertDirectory struct {
	CAPool *x509.CertPool
	Key    *rsa.PrivateKey
	//store   Store
	tlsConf common.TLSConfig
	Cert    *x509.Certificate
}

const (
	LEVELDB = "leveldb"
)

func NewCertDirectory(tlsConf common.TLSConfig, store Store) (cd *CertDirectory, err error) {
	var key *rsa.PrivateKey
	if common.FileExists(tlsConf.Key) {
		keyPEM, err := ioutil.ReadFile(tlsConf.Key)
		if err != nil {
			return nil, fmt.Errorf("error loading private key: %v", err)

		}

		key, err = pki.PemToPrivateKey(keyPEM)
		if err != nil {
			return nil, fmt.Errorf("error parsing private key: %v", err)
		}
	} else {
		return nil, fmt.Errorf("error loading private key: File does not exist")
	}
	caPEM, err := ioutil.ReadFile(tlsConf.CA)
	if err != nil {
		return nil, fmt.Errorf("error loading ca PEM: %v", err)
	}
	certPool := x509.NewCertPool()
	// Append the certificates from the CA
	if ok := certPool.AppendCertsFromPEM(caPEM); !ok {
		return nil, errors.New("failed to append ca certs")
	}

	CertPEM, err := ioutil.ReadFile(tlsConf.Cert)
	if err != nil {
		return nil, fmt.Errorf("error loading ca PEM: %v", err)
	}
	cert, err := pki.PEMToCertificate(CertPEM)
	if err != nil {
		return nil, fmt.Errorf("error loading parsing cert PEM: %v", err)
	}
	cd = &CertDirectory{Key: key, CAPool: certPool, tlsConf: tlsConf, Cert: cert}

	return cd, nil
}
