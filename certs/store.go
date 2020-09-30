package certs

import (
	"crypto/x509"

	"github.com/linksmart/historical-datastore/common"
)

var (
	ErrNotFound = &common.NotFoundError{S: "not found"}
	ErrConflict = &common.ConflictError{S: "conflict"}
)

type Store interface {
	add(url string, certificate x509.Certificate) error
	fetch(url string) (certificate *x509.Certificate, err error)
	delete(url string) error
}
