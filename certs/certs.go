package certs

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/linksmart/hds-data-synchronizer/common"
	common2 "github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/pki"
)

type CertDirectory struct {
	CAPool  *x509.CertPool
	Key     *rsa.PrivateKey
	store   Store
	tlsConf common.TLSConfig
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
		key, err = rsa.GenerateKey(rand.Reader, 2024)
		if err != nil {
			return nil, fmt.Errorf("error generating private key: %v", err)

		}
		privKeyBytes, err := pki.PrivateKeyToPEM(key)
		if err != nil {
			return nil, fmt.Errorf("error converting private key to PEM %v", err)
		}

		err = ioutil.WriteFile(tlsConf.Key, privKeyBytes, 0600)
		if err != nil {
			return nil, fmt.Errorf("error writing private key %v", err)
		}
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

	cd = &CertDirectory{Key: key, CAPool: certPool, tlsConf: tlsConf, store: store}

	return cd, nil
}

func (cd CertDirectory) GetCert(hdsUrl string, caEndpoint string) (*x509.Certificate, error) {
	//TODO: logic to return the existing one.
	cert, err := cd.store.fetch(hdsUrl)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("error fetching from storage: %v", err)
	}

	if cert != nil {
		return cert, nil
	}
	hdsUrl = strings.TrimRight(hdsUrl, "/")
	csr, err := makeCSR(cd.tlsConf.CertData, *cd.Key)
	if err != nil {
		return nil, fmt.Errorf("error making csr: %v", err)
	}

	res, err := http.Post(caEndpoint, "application/x-pem-file", bytes.NewReader(csr))
	if err != nil {
		return nil, fmt.Errorf("error posting to %s: %v", endpoint, err)
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from %s, %d", endpoint, res.StatusCode)
	}

	certPEM, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading the result body: %v", err)
	}
	cert, err = pki.PEMToCertificate(certPEM)
	if err != nil {
		return nil, fmt.Errorf("error decoding cert pem from %s: %v", endpoint, err)
	}
	err = cd.store.add(hdsUrl, *cert)
	if err != nil {
		return nil, fmt.Errorf("error storing cert pem for %s: %v", hdsUrl, err)
	}
	return cert, nil
}

func (cd CertDirectory) Remove(hdsUrl string) error {
	return cd.store.delete(hdsUrl)
}

func makeCSR(certData common2.CertData, key rsa.PrivateKey) (pemBytes []byte, err error) {
	template := new(x509.CertificateRequest)

	template.Subject = pkix.Name{
		Country:            []string{certData.Country},
		Province:           []string{certData.Province},
		Locality:           []string{certData.Locality},
		Organization:       []string{certData.Organization},
		OrganizationalUnit: []string{certData.OrganizationalUnit},
		CommonName:         certData.CommonName,
	}
	template.DNSNames = strings.Split(certData.DNSNames, ",")
	ipAddresses := strings.Split(certData.IPAddresses, ",")

	var ips []net.IP
	for _, v := range ipAddresses {
		ips = append(ips, net.ParseIP(v))
	}
	template.IPAddresses = ips

	template.PublicKey = &key

	csr, err := x509.CreateCertificateRequest(rand.Reader, template, &key)
	if err != nil {
		return nil, err
	}
	return pki.CSRASN1ToPEM(csr)
}

func SupportedBackends(bType string) bool {
	if bType == LEVELDB {
		return true
	} else {
		return false
	}
}
