package common

import "github.com/linksmart/historical-datastore/common"

type Config struct {
	//HDS is the url of the source HDS
	HDS string `json:"hds"`

	//TDD is the url of TDD
	TDD string `json:"tdd"`

	//TLS configuration
	TLS TLSConfig `json:"tls"`
}

type TLSConfig struct {
	CA          string          `json:"ca"`
	Key         string          `json:"key"`
	SourceHDSCA string          `json:"sourceHdsCa"` //This is the url of source HDS's CA.
	Storage     StorageConfig   `json:"storage"`
	CertData    common.CertData `json:"certData"`
}
type StorageConfig struct {
	DSN  string `json:"dsn"`
	Type string `json:"type"`
}
