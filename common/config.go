package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Certificate authority and Certificate and

	Destination  string    `json:"destination"`
	SyncInterval string    `json:"syncInterval"`
	Source       string    `json:"source"`
	TLS          TLSConfig `json:"tls"`
}

type TLSConfig struct {
	// CA is the  Certificate of CA
	CA string `json:"ca"`
	// key is the private key of the client
	Key string `json:"key"`
	// SourceHDSCA is the url of source HDS's CA.
	Cert string `json:"cert"`
}

// loads service configuration from a file at the given path
func LoadConfig(confPath *string) (*Config, error) {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return nil, err
	}

	var conf Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}

	// Override loaded values with environment variables
	err = envconfig.Process("sync", &conf)
	if err != nil {
		return nil, err
	}

	if conf.Source == "" || conf.Destination == "" {
		return nil, fmt.Errorf("HDS source and destionation endpoints have to be defined")
	}
	sourceUrl, err := url.Parse(conf.Source)
	if err != nil {
		return nil, fmt.Errorf("HDS source should be a valid URL")
	}
	if sourceUrl.Host == "" {
		return nil, fmt.Errorf("missing schema or hostname from HDS souece")
	}

	destUrl, err := url.Parse(conf.Destination)
	if err != nil {
		return nil, fmt.Errorf("HDS destination should be a valid URL")
	}
	if destUrl.Host == "" {
		return nil, fmt.Errorf("missing schema or hostname from HDS destination")
	}

	return &conf, nil
}
