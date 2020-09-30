package certs

import (
	"crypto/x509"
	"fmt"
	"net/url"
	"sync"

	"github.com/linksmart/historical-datastore/pki"
	"github.com/syndtr/goleveldb/leveldb"
)

// LevelDB storage
type LevelDBStorage struct {
	db *leveldb.DB
	wg sync.WaitGroup
}

func NewLevelDBStorage(DSN string) (*LevelDBStorage, func() error, error) {
	url, err := url.Parse(DSN)
	if err != nil {
		return nil, nil, err
	}

	// Open the database
	db, err := leveldb.OpenFile(url.Path, nil)
	if err != nil {
		return nil, nil, err
	}

	s := &LevelDBStorage{
		db: db,
	}
	return s, s.close, nil
}

func (s *LevelDBStorage) close() error {
	// Wait for pending operations
	s.wg.Wait()
	return s.db.Close()
}

func (s *LevelDBStorage) add(url string, certificate x509.Certificate) error {
	s.wg.Add(1)
	defer s.wg.Done()
	if has, _ := s.db.Has([]byte(url), nil); has {
		return fmt.Errorf("%w: Resource name not unique: %s", ErrConflict, url)
	}
	// GetCert the new pem to database
	pem, err := pki.CertificateToPEM(certificate)
	err = s.db.Put([]byte(url), pem, nil)
	return err
}

func (s *LevelDBStorage) fetch(url string) (*x509.Certificate, error) {
	s.wg.Add(1)
	defer s.wg.Done()
	pem, err := s.db.Get([]byte(url), nil)
	if err == leveldb.ErrNotFound {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, err)
	} else if err != nil {
		return nil, err
	}
	return pki.PEMToCertificate(pem)

}

func (s *LevelDBStorage) delete(url string) error {
	s.wg.Add(1)
	defer s.wg.Done()
	err := s.db.Delete([]byte(url), nil)
	if err == leveldb.ErrNotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, err)
	} else if err != nil {
		return err
	}
	return nil
}
