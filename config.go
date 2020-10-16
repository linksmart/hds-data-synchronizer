package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/kelseyhightower/envconfig"
	"github.com/linksmart/hds-data-synchronizer/common"
)

// loads service configuration from a file at the given path
func loadConfig(confPath *string) (*common.Config, error) {
	file, err := ioutil.ReadFile(*confPath)
	if err != nil {
		return nil, err
	}

	var conf common.Config
	err = json.Unmarshal(file, &conf)
	if err != nil {
		return nil, err
	}

	// Override loaded values with environment variables
	err = envconfig.Process("sync", &conf)
	if err != nil {
		return nil, err
	}

	if conf.HDS == "" || conf.TDDConf.Endpoint == "" {
		return nil, fmt.Errorf("HDS and TDD endpoints have to be defined")
	}
	hdsUrl, err := url.Parse(conf.HDS)
	if err != nil {
		return nil, fmt.Errorf("HDS endpoint should be a valid URL")
	}
	if hdsUrl.Host == "" {
		return nil, fmt.Errorf("missing schema or hostname from HDS endpoint")
	}
	_, err = url.Parse(conf.TDDConf.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("TDD endpoint should be a valid URL")
	}

	if common.FileExists(conf.TLS.CA) == false {
		return nil, fmt.Errorf("CA file '%s' does not exist", conf.TLS.CA)
	}

	if common.FileExists(conf.TLS.Key) == false {
		return nil, fmt.Errorf("Key file '%s' does not exist", conf.TLS.Key)
	}
	if common.FileExists(conf.TLS.Cert) == false {
		return nil, fmt.Errorf("Cert file '%s' does not exist", conf.TLS.Cert)
	}
	return &conf, nil
}
