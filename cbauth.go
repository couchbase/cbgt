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
	DisableNonSSLPorts bool // place holder for future, not used yet
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

// ConfigRefreshNotifier defines the SecuritySetting's refresh
// callback signature
type ConfigRefreshNotifier func() error

func RegisterConfigRefreshCallback(key string, cb ConfigRefreshNotifier) {
	securityCtx.mutex.Lock()
	securityCtx.notifiers[key] = cb
	securityCtx.mutex.Unlock()
	log.Printf("cbauth: key: %s registered for tls config updates", key)
}

func (c *SecurityContext) refresh(code uint64) error {
	log.Printf("cbauth: received  security change notification, code: %v", code)

	newSetting := &SecuritySetting{}
	hasEnabled := false

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
		hasEnabled = oldSetting.EncryptionEnabled
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
	// dcp feeds, http servers and grpc servers.
	// We are notifying every certificate change irrespective of
	// the encryption status.
	if hasEnabled || hasEnabled != newSetting.EncryptionEnabled ||
		code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		for key, notifier := range c.notifiers {
			go func(key string, notify ConfigRefreshNotifier) {
				log.Printf("cbauth: notifying configs change for key: %v", key)
				if err := notify(); err != nil {
					log.Errorf("cbauth: notify failed, for key: %v: err: %v", key, err)
				}
			}(key, notifier)
		}
	} else {
		log.Printf("cbauth: encryption not enabled")
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

	if err = updateGocbcoreSecurityConfig(cfg.EncryptData); err != nil {
		log.Warnf("cbauth: Error updating gocbcore's TLS data, err: %v", err)
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

	return nil
}

// ----------------------------------------------------------------

// security config for gocbcore DCP Agents
type gocbcoreSecurityConfig struct {
	encryptData bool
	rootCAs     *x509.CertPool
}

var currGocbcoreSecurityConfigMutex sync.RWMutex
var currGocbcoreSecurityConfig *gocbcoreSecurityConfig

func init() {
	currGocbcoreSecurityConfig = &gocbcoreSecurityConfig{}
}

func updateGocbcoreSecurityConfig(encryptData bool) error {
	currGocbcoreSecurityConfigMutex.Lock()
	defer currGocbcoreSecurityConfigMutex.Unlock()

	currGocbcoreSecurityConfig.encryptData = encryptData
	if encryptData && len(TLSCertFile) > 0 {
		certInBytes, err := ioutil.ReadFile(TLSCertFile)
		if err != nil {
			return err
		}

		rootCAs := x509.NewCertPool()
		ok := rootCAs.AppendCertsFromPEM(certInBytes)
		if !ok {
			return fmt.Errorf("error appending certificates")
		}

		currGocbcoreSecurityConfig.rootCAs = rootCAs
	}

	return nil
}

func FetchGocbcoreSecurityConfig() *x509.CertPool {
	var rootCAs *x509.CertPool
	currGocbcoreSecurityConfigMutex.RLock()
	if currGocbcoreSecurityConfig.encryptData {
		rootCAs = currGocbcoreSecurityConfig.rootCAs
	}
	currGocbcoreSecurityConfigMutex.RUnlock()
	return rootCAs
}
