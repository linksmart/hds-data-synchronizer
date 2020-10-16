package common

import "github.com/linksmart/go-sec/auth/obtainer"

type Config struct {
	// HDS is the url of the source HDS
	HDS string `json:"hds"`

	// TDD is the url of TDD
	TDDConf TDDConf `json:"tddConf"`

	// TLS configuration
	TLS TLSConfig `json:"tls"`
}

type TDDConf struct {
	Endpoint string        `json:"endpoint"`
	Auth     obtainer.Conf `json:"auth"`
}
type TLSConfig struct {
	// CA is the  Certificate of CA
	CA string `json:"ca"`
	// key is the private key of the client
	Key string `json:"key"`
	// SourceHDSCA is the url of source HDS's CA.
	Cert string `json:"cert"`
}
