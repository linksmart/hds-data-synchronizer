// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/linksmart/historical-datastore/registry"
)

const (
	mqttRetryInterval = 10 // seconds
)

type MQTTConnector struct {
	sync.Mutex
	registry registry.Controller
	storage  Storage
	clientID string
	managers map[string]*Manager
	// cache of resource->ts
	cache map[string]*registry.TimeSeries
	// failed mqtt registrations
	failedRegistrations map[string]*registry.MQTTSource
}

type Manager struct {
	url    string
	client paho.Client
	// connector *MQTTConnector
	// total subscriptions for each topic in this manager
	subscriptions map[string]*Subscription
}

type Subscription struct {
	connector *MQTTConnector
	url       string
	topic     string
	qos       byte
	receivers int
}

func NewMQTTConnector(storage Storage, clientID string) (*MQTTConnector, error) {
	c := &MQTTConnector{
		storage:             storage,
		clientID:            clientID,
		managers:            make(map[string]*Manager),
		cache:               make(map[string]*registry.TimeSeries),
		failedRegistrations: make(map[string]*registry.MQTTSource),
	}
	return c, nil
}

func (c *MQTTConnector) Start(reg registry.Controller) error {
	c.registry = reg

	perPage := 100
	for page := 1; ; page++ {
		series, total, err := c.registry.GetMany(page, perPage)
		if err != nil {
			return fmt.Errorf("MQTT: Error getting time series: %v", err)
		}

		for _, ts := range series {
			if ts.Source.SrcType == registry.Mqtt {
				err := c.register(*ts.Source.MQTTSource)
				if err != nil {
					log.Printf("MQTT: Error registering subscription: %v. Retrying in %ds", err, mqttRetryInterval)
					c.failedRegistrations[ts.Name] = ts.Source.MQTTSource
				}
			}
		}

		if page*perPage >= total {
			break
		}
	}

	go c.retryRegistrations()

	return nil
}

func (c *MQTTConnector) flushCache() {
	c.cache = make(map[string]*registry.TimeSeries)
}

func (c *MQTTConnector) retryRegistrations() {
	for {
		time.Sleep(mqttRetryInterval * time.Second)
		c.Lock()
		for id, mqttSource := range c.failedRegistrations {
			err := c.register(*mqttSource)
			if err != nil {
				log.Printf("MQTT: Error registering subscription: %v. Retrying in %ds", err, mqttRetryInterval)
				continue
			}
			delete(c.failedRegistrations, id)
		}
		c.Unlock()
	}
}

func (c *MQTTConnector) register(source registry.MQTTSource) error {

	if _, exists := c.managers[source.BrokerURL]; !exists { // NO CLIENT FOR THIS BROKER
		manager := &Manager{
			url:           source.BrokerURL,
			subscriptions: make(map[string]*Subscription),
		}

		manager.subscriptions[source.Topic] = &Subscription{
			connector: c,
			url:       source.BrokerURL,
			topic:     source.Topic,
			qos:       source.QoS,
			receivers: 1,
		}

		opts := paho.NewClientOptions() // uses defaults: https://godoc.org/github.com/eclipse/paho.mqtt.golang#NewClientOptions
		opts.AddBroker(source.BrokerURL)
		opts.SetClientID(fmt.Sprintf("HDS-%s", c.clientID))
		opts.SetOnConnectHandler(manager.onConnectHandler)
		opts.SetConnectionLostHandler(manager.onConnectionLostHandler)
		if source.Username != "" {
			opts.SetUsername(source.Username)
		}
		if source.Password != "" {
			opts.SetPassword(source.Password)
		}
		tlsConfig, err := pahoTLSConfig(source.CaFile, source.CertFile, source.KeyFile, source.Insecure)
		if err != nil {
			return fmt.Errorf("MQTT: Error configuring TLS options for broker %v: %v", source.BrokerURL, err)
		}
		opts.SetTLSConfig(tlsConfig)

		manager.client = paho.NewClient(opts)

		if token := manager.client.Connect(); token.Wait() && token.Error() != nil {
			return fmt.Errorf("MQTT: Error connecting to broker %v: %v", source.BrokerURL, token.Error())
		}
		c.managers[source.BrokerURL] = manager

	} else { // THERE IS A CLIENT FOR THIS BROKER
		manager := c.managers[source.BrokerURL]

		// TODO: check if another wildcard subscription matches the topic.
		if _, exists := manager.subscriptions[source.Topic]; !exists { // NO SUBSCRIPTION FOR THIS TOPIC
			subscription := &Subscription{
				connector: c,
				url:       source.BrokerURL,
				topic:     source.Topic,
				qos:       source.QoS,
				receivers: 1,
			}
			// Subscribe
			if token := manager.client.Subscribe(subscription.topic, subscription.qos, subscription.onMessage); token.Wait() && token.Error() != nil {
				return fmt.Errorf("MQTT: Error subscribing: %v", token.Error())
			}
			manager.subscriptions[source.Topic] = subscription
			log.Printf("MQTT: %s: Subscribed to %s", source.BrokerURL, source.Topic)

		} else { // There is a subscription for this topic
			//log.Printf("MQTT: %s: Already subscribed to %s", mqttConf.BrokerURL, mqttConf.Topic)
			manager.subscriptions[source.Topic].receivers++
		}
	}

	return nil
}

func (c *MQTTConnector) unregister(mqttSource *registry.MQTTSource) error {
	manager := c.managers[mqttSource.BrokerURL]
	// There may be no subscriptions due to a failed registration when HDS is restarted
	if manager == nil {
		return nil
	}
	manager.subscriptions[mqttSource.Topic].receivers--

	if manager.subscriptions[mqttSource.Topic].receivers == 0 {
		// Unsubscribe
		if token := manager.client.Unsubscribe(mqttSource.Topic); token.Wait() && token.Error() != nil {
			return fmt.Errorf("MQTT: Error unsubscribing: %v", token.Error())
		}
		delete(manager.subscriptions, mqttSource.Topic)
		log.Printf("MQTT: %s: Unsubscribed from %s", mqttSource.BrokerURL, mqttSource.Topic)
	}
	if len(manager.subscriptions) == 0 {
		// Disconnect
		manager.client.Disconnect(250)
		delete(c.managers, mqttSource.BrokerURL)
		log.Printf("MQTT: %s: Disconnected!", mqttSource.BrokerURL)
	}

	return nil
}

func (m *Manager) onConnectHandler(client paho.Client) {
	log.Printf("MQTT: %s: Connected.", m.url)
	m.client = client
	for _, subscription := range m.subscriptions {
		if token := m.client.Subscribe(subscription.topic, subscription.qos, subscription.onMessage); token.Wait() && token.Error() != nil {
			log.Printf("MQTT: %s: Error subscribing: %v", m.url, token.Error())
		}
		log.Printf("MQTT: %s: Subscribed to %s", m.url, subscription.topic)
	}
}

func (m *Manager) onConnectionLostHandler(client paho.Client, err error) {
	log.Printf("MQTT: %s: Connection lost: %v", m.url, err)
}

func (s *Subscription) onMessage(client paho.Client, msg paho.Message) {
	t1 := time.Now()

	logHeader := fmt.Sprintf("\"SUB %s MQTT/QOS%d\"", msg.Topic(), msg.Qos())
	logMQTTError := func(code int, format string, v ...interface{}) {
		log.Printf("%s %d %v %s", logHeader, code, time.Now().Sub(t1), fmt.Sprintf(format, v...))
	}

	senmlPack, err := codec.Decode(senml.MediaTypeSenmlJSON, msg.Payload())
	if err != nil {
		logMQTTError(http.StatusBadRequest, "Error parsing json: %s : %v", msg.Payload(), err)
		return
	}

	// Fill the data map with provided data points
	senmlPack.Normalize()
	data := make(map[string]senml.Pack)
	series := make(map[string]*registry.TimeSeries)
	for _, r := range senmlPack {
		// Find the time series for this entry
		ts, exists := s.connector.cache[r.Name]
		if !exists {
			ts, err = s.connector.registry.Get(r.Name)
			if err != nil {
				if errors.Is(err, registry.ErrNotFound) {
					logMQTTError(http.StatusNotFound, "Warning: Resource not found: %v", r.Name)
					continue
				}
				logMQTTError(http.StatusInternalServerError, "Error finding resource: %v", r.Name)
				continue
			}

			s.connector.cache[r.Name] = ts
		}

		// Check if the message is wanted
		if ts.Source.MQTTSource == nil {
			logMQTTError(http.StatusNotAcceptable, "Ignoring unwanted message for resource: %v", r.Name)
			continue
		}
		if ts.Source.MQTTSource.BrokerURL != s.url {
			logMQTTError(http.StatusNotAcceptable, "Ignoring message from unwanted broker %v for time series: %v", s.url, r.Name)
			continue
		}
		if ts.Source.MQTTSource.Topic != s.topic {
			logMQTTError(http.StatusNotAcceptable, "Ignoring message with unwanted topic %v for time series: %v", s.topic, r.Name)
			continue
		}

		err := validateRecordAgainstRegistry(r, ts)

		if err != nil {
			logMQTTError(http.StatusBadRequest,
				fmt.Sprintf("Error validating the record:%v", err))
			return
		}

		_, ok := data[ts.Name]
		if !ok {
			data[ts.Name] = []senml.Record{}
			series[ts.Name] = ts
		}
		data[ts.Name] = append(data[ts.Name], r)
	}

	if len(data) > 0 {
		// Add data to the storage
		err = s.connector.storage.Submit(context.Background(), data, series)
		if err != nil {
			logMQTTError(http.StatusInternalServerError, "Error writing data to the database: %v", err)
			return
		}

		log.Printf("%s %d %v\n", logHeader, http.StatusAccepted, time.Now().Sub(t1))
	}
}

// NOTIFICATION HANDLERS

// CreateHandler handles the creation of a new time series
func (c *MQTTConnector) CreateHandler(ts registry.TimeSeries) error {
	c.Lock()
	defer c.Unlock()

	if ts.Source.MQTTSource != nil {
		err := c.register(*ts.Source.MQTTSource)
		if err != nil {
			return fmt.Errorf("MQTT: Error adding subscription: %v", err)
		}
	}

	return nil
}

// UpdateHandler handles updates of a time series
func (c *MQTTConnector) UpdateHandler(oldTs registry.TimeSeries, newTS registry.TimeSeries) error {
	c.Lock()
	defer c.Unlock()

	if oldTs.Source.MQTTSource != newTS.Source.MQTTSource {
		// Remove old subscription
		if oldTs.Source.MQTTSource != nil {
			err := c.unregister(oldTs.Source.MQTTSource)
			if err != nil {
				return fmt.Errorf("MQTT: Error removing subscription: %v", err)
			}
		}
		delete(c.failedRegistrations, oldTs.Name)
		// Add new subscription
		if newTS.Source.MQTTSource != nil {
			err := c.register(*newTS.Source.MQTTSource)
			if err != nil {
				return fmt.Errorf("MQTT: Error adding subscription: %v", err)
			}
		}
	}
	return nil
}

// DeleteHandler handles deletion of a time series
func (c *MQTTConnector) DeleteHandler(oldTS registry.TimeSeries) error {
	c.Lock()
	defer c.Unlock()

	c.flushCache()

	// Remove old subscription
	if oldTS.Source.MQTTSource != nil {
		delete(c.cache, oldTS.Name)
		err := c.unregister(oldTS.Source.MQTTSource)
		if err != nil {
			return fmt.Errorf("MQTT: Error removing subscription: %v", err)
		}
		delete(c.failedRegistrations, oldTS.Name)
	}
	return nil
}

func pahoTLSConfig(caFile, certFile, keyFile string, insecure bool) (*tls.Config, error) {

	tlsConfig := &tls.Config{}
	if caFile != "" {
		// Import trusted certificates from CAfile.pem.
		// Alternatively, manually add CA certificates to
		// default openssl CA bundle.
		tlsConfig.RootCAs = x509.NewCertPool()
		pemCerts, err := ioutil.ReadFile(caFile)
		if err == nil {
			tlsConfig.RootCAs.AppendCertsFromPEM(pemCerts)
		}
	}
	if certFile != "" && keyFile != "" {
		// Import client certificate/key pair
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("error loading client keypair: %s", err)
		}
		// Just to print out the client certificate..
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("error parsing client certificate: %s", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	tlsConfig.InsecureSkipVerify = insecure

	return tlsConfig, nil
}