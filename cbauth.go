//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase/cbdatasource"
)

func init() {
	securityCtx = &SecurityContext{
		notifiers: make(map[string]ConfigRefreshNotifier),
	}
}

var TLSCertFile string
var TLSKeyFile string

type SecuritySetting struct {
	EncryptionEnabled  bool
	DisableNonSSLPorts bool
	Certificate        *tls.Certificate
	CertInBytes        []byte
	TLSConfig          *cbauth.TLSConfig
	ClientAuthType     *tls.ClientAuthType
}

var currentSetting unsafe.Pointer = unsafe.Pointer(new(SecuritySetting))

func GetSecuritySetting() *SecuritySetting {
	return (*SecuritySetting)(atomic.LoadPointer(&currentSetting))
}

var securityCtx *SecurityContext

// RegisterSecurityNotifications registers for the cbauth's security callbacks
func RegisterSecurityNotifications() {
	cbauth.RegisterConfigRefreshCallback(securityCtx.refresh)
}

// SecurityContext let us register multiple tls config
// update callbacks and acts as a wrapper for handling
// config changes.
type SecurityContext struct {
	mutex     sync.RWMutex
	notifiers map[string]ConfigRefreshNotifier
}

const (
	AuthChange_encryption = 1 << iota
	AuthChange_nonSSLPorts
	AuthChange_certificates
)

// ConfigRefreshNotifier defines the SecuritySetting's refresh
// callback signature
type ConfigRefreshNotifier func(status int) error

func RegisterConfigRefreshCallback(key string, cb ConfigRefreshNotifier) {
	securityCtx.mutex.Lock()
	securityCtx.notifiers[key] = cb
	securityCtx.mutex.Unlock()
	log.Printf("cbauth: key: %s registered for tls config updates", key)
}

func (c *SecurityContext) refresh(code uint64) error {
	log.Printf("cbauth: received security change notification, code: %v", code)

	newSetting := &SecuritySetting{}
	var encryptionEnabled, disableNonSSLPorts bool

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
		encryptionEnabled = oldSetting.EncryptionEnabled
		disableNonSSLPorts = oldSetting.DisableNonSSLPorts
	}

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := c.refreshConfig(newSetting); err != nil {
			return err
		}

		if err := c.refreshCert(newSetting); err != nil {
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		if err := c.refreshEncryption(newSetting); err != nil {
			return err
		}
	}

	atomic.StorePointer(&currentSetting, unsafe.Pointer(newSetting))

	c.mutex.RLock()
	// This will notify tls config changes to all the subscribers like
	// dcp feeds, http servers and grpc servers;
	// Notifying every certificate change irrespective of the encryption status.
	var status int
	if encryptionEnabled != newSetting.EncryptionEnabled {
		status |= AuthChange_encryption
	}
	if disableNonSSLPorts != newSetting.DisableNonSSLPorts {
		status |= AuthChange_nonSSLPorts
	}
	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		status |= AuthChange_certificates
	}

	if status != 0 {
		for key, notifier := range c.notifiers {
			go func(key string, notify ConfigRefreshNotifier) {
				log.Printf("cbauth: notifying configs change for key: %v", key)
				if err := notify(status); err != nil {
					log.Errorf("cbauth: notify failed, for key: %v: err: %v", key, err)
				}
			}(key, notifier)
		}
	} else {
		log.Printf("cbauth: encryption settings not affected")
	}
	c.mutex.RUnlock()

	return nil
}

func (c *SecurityContext) refreshConfig(configs *SecuritySetting) error {
	TLSConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		log.Warnf("cbauth: GetTLSConfig failed, err: %v", err)
		return err
	}

	ClientAuthType, err := cbauth.GetClientCertAuthType()
	if err != nil {
		log.Warnf("cbauth: GetClientCertAuthType failed, err: %v", err)
		return err
	}

	configs.TLSConfig = &TLSConfig
	configs.ClientAuthType = &ClientAuthType

	return nil
}

func (c *SecurityContext) refreshCert(configs *SecuritySetting) error {
	if len(TLSCertFile) == 0 || len(TLSKeyFile) == 0 {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(TLSCertFile, TLSKeyFile)
	if err != nil {
		log.Printf("cbauth: LoadX509KeyPair err : %v", err)
		return err
	}

	certInBytes, err := ioutil.ReadFile(TLSCertFile)
	if err != nil {
		log.Errorf("cbauth: Certificates read err: %v", err)
		return err
	}

	configs.Certificate = &cert
	configs.CertInBytes = certInBytes

	return nil
}

func (c *SecurityContext) refreshEncryption(configs *SecuritySetting) error {
	cfg, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		log.Warnf("cbauth: GetClusterEncryptionConfig err: %v", err)
		return err
	}

	configs.EncryptionEnabled = cfg.EncryptData
	configs.DisableNonSSLPorts = cfg.DisableNonSSLPorts

	if err = updateSecurityConfig(cfg.EncryptData); err != nil {
		log.Warnf("cbauth: Error updating TLS data, err: %v", err)
		return err
	}

	if err = cbdatasource.UpdateSecurityConfig(&cbdatasource.SecurityConfig{
		EncryptData:        cfg.EncryptData,
		DisableNonSSLPorts: cfg.DisableNonSSLPorts,
		CertFile:           TLSCertFile,
		KeyFile:            TLSKeyFile,
	}); err != nil {
		log.Warnf("cbauth: Error updating go-couchbase/cbdatasource's"+
			" TLS data, err: %v", err)
		return err
	}

	// Close all cached couchbase.Bucket instances, so new ones can be
	// setup with the new config.
	statsCBBktMap.closeAllCouchbaseBuckets()

	return nil
}

// ----------------------------------------------------------------

// security config for gocbcore DCP Agents
type securityConfig struct {
	encryptData bool
	rootCAs     *x509.CertPool
}

var currSecurityConfigMutex sync.RWMutex
var currSecurityConfig *securityConfig

func init() {
	currSecurityConfig = &securityConfig{}
}

func updateSecurityConfig(encryptData bool) error {
	currSecurityConfigMutex.Lock()
	defer currSecurityConfigMutex.Unlock()

	currSecurityConfig.encryptData = encryptData
	if encryptData {
		rootCAs := LoadCertFromTLSCertFile()
		if rootCAs == nil {
			return fmt.Errorf("error obtaining certificate")
		}
		currSecurityConfig.rootCAs = rootCAs
	}

	// force reconnect cached gocbcore.Agents and DCPAgents
	statsAgentsMap.forceReconnectAgents()
	dcpAgentMap.forceReconnectAgents()

	return nil
}

func FetchSecurityConfig() *x509.CertPool {
	var rootCAs *x509.CertPool
	currSecurityConfigMutex.RLock()
	if currSecurityConfig.encryptData {
		rootCAs = currSecurityConfig.rootCAs
	}
	currSecurityConfigMutex.RUnlock()
	return rootCAs
}

func LoadCertFromTLSCertFile() *x509.CertPool {
	var rootCAs *x509.CertPool
	if len(TLSCertFile) > 0 {
		certInBytes, err := ioutil.ReadFile(TLSCertFile)
		if err != nil {
			log.Warnf("LoadCertFromTLSCertFile, err: %v", err)
			return nil
		} else {
			rootCAs = x509.NewCertPool()
			ok := rootCAs.AppendCertsFromPEM(certInBytes)
			if !ok {
				log.Warnf("LoadCertFromTLSCertFile, error appending certificates")
				return nil
			}
		}
	}

	return rootCAs
}
