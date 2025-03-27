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
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/cbauthimpl"
	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase/cbdatasource"
	cbtls "github.com/couchbase/goutils/tls"
)

func init() {
	securityCtx = &SecurityContext{
		notifiers: make(map[string]ConfigRefreshNotifier),
	}
}

var TLSCAFile string
var TLSCertFile string
var TLSKeyFile string

var ClientCertFile string
var ClientKeyFile string

type SecuritySetting struct {
	TLSConfig                  *cbauth.TLSConfig
	ClientAuthType             *tls.ClientAuthType
	EncryptionEnabled          bool
	DisableNonSSLPorts         bool
	ShouldClientsUseClientCert bool

	ServerCertificate tls.Certificate
	CACertInBytes     []byte

	ClientCertificate tls.Certificate
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
	AuthChange_clientCertificates
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

	newSetting := SecuritySetting{}
	var encryptionEnabled, disableNonSSLPorts bool

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = temp
		encryptionEnabled = oldSetting.EncryptionEnabled
		disableNonSSLPorts = oldSetting.DisableNonSSLPorts
	}

	if code&cbauthimpl.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := c.refreshConfigAndCert(&newSetting); err != nil {
			return err
		}
	}

	if code&cbauthimpl.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		if err := c.refreshEncryption(&newSetting); err != nil {
			return err
		}
	}

	if code&cbauthimpl.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0 {
		if err := c.refreshClientCert(&newSetting); err != nil {
			return err
		}
	}

	atomic.StorePointer(&currentSetting, unsafe.Pointer(&newSetting))

	if code&cbauthimpl.CFG_CHANGE_CERTS_TLSCONFIG != 0 ||
		encryptionEnabled != newSetting.EncryptionEnabled ||
		code&cbauthimpl.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0 {
		if err := c.refreshClients(GetSecuritySetting()); err != nil {
			return err
		}
	}

	c.mutex.RLock()
	// This will notify tlsConfig/limits changes to all the subscribers like
	// dcp feeds, http servers and grpc servers;
	// Notifying every certificate change irrespective of the encryption status.
	var status int
	if encryptionEnabled != newSetting.EncryptionEnabled {
		status |= AuthChange_encryption
	}
	if disableNonSSLPorts != newSetting.DisableNonSSLPorts {
		status |= AuthChange_nonSSLPorts
	}
	if code&cbauthimpl.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		status |= AuthChange_certificates
	}
	if code&cbauthimpl.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0 {
		status |= AuthChange_clientCertificates
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

func (c *SecurityContext) refreshConfigAndCert(configs *SecuritySetting) error {
	tlsConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		log.Warnf("cbauth: GetTLSConfig failed, err: %v", err)
		return err
	}

	clientAuthType, err := cbauth.GetClientCertAuthType()
	if err != nil {
		log.Warnf("cbauth: GetClientCertAuthType failed, err: %v", err)
		return err
	}

	configs.TLSConfig = &tlsConfig
	configs.ClientAuthType = &clientAuthType

	if len(TLSCAFile) == 0 || len(TLSCertFile) == 0 || len(TLSKeyFile) == 0 {
		return nil
	}

	var privateKeyPassphrase []byte
	if configs.TLSConfig != nil {
		privateKeyPassphrase = configs.TLSConfig.PrivateKeyPassphrase
	}

	cert, err := cbtls.LoadX509KeyPair(TLSCertFile, TLSKeyFile, privateKeyPassphrase)
	if err != nil {
		log.Errorf("cbauth: LoadX509KeyPair err: %v", err)
		return err
	}

	certInBytes, err := os.ReadFile(TLSCAFile)
	if err != nil {
		log.Errorf("cbauth: Certificate read err: %v", err)
		return err
	}

	configs.ServerCertificate = cert
	configs.CACertInBytes = certInBytes

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

	return nil
}

func (c *SecurityContext) refreshClientCert(configs *SecuritySetting) error {
	if len(ClientCertFile) == 0 || len(ClientKeyFile) == 0 {
		return nil
	}

	var clientPrivateKeyPassPhrase []byte
	if configs.TLSConfig != nil {
		clientPrivateKeyPassPhrase = configs.TLSConfig.ClientPrivateKeyPassphrase
	}

	cert, err := cbtls.LoadX509KeyPair(ClientCertFile, ClientKeyFile,
		clientPrivateKeyPassPhrase)
	if err != nil {
		log.Errorf("cbauth: LoadX509KeyPair (client cert) err: %v", err)
	}

	configs.ClientCertificate = cert

	tlsConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		log.Warnf("cbauth: GetTLSConfig failed, err: %v", err)
		return err
	}

	configs.ShouldClientsUseClientCert = tlsConfig.ShouldClientsUseClientCert

	return nil
}

func (c *SecurityContext) refreshClients(configs *SecuritySetting) error {
	if err := updateSecurityConfig(
		configs.EncryptionEnabled, configs.CACertInBytes); err != nil {
		log.Warnf("cbauth: Error updating TLS data, err: %v", err)
		return err
	}

	sc := &cbdatasource.SecurityConfig{
		EncryptData:        configs.EncryptionEnabled,
		DisableNonSSLPorts: configs.DisableNonSSLPorts,
		RootCAs:            FetchRootCAs(),
	}
	if configs.ShouldClientsUseClientCert {
		sc.Certificates = []tls.Certificate{configs.ClientCertificate}
	}

	if err := cbdatasource.UpdateSecurityConfig(sc); err != nil {
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

func updateSecurityConfig(encryptData bool, caCertInBytes []byte) error {
	currSecurityConfigMutex.Lock()
	defer currSecurityConfigMutex.Unlock()

	currSecurityConfig.encryptData = encryptData
	if encryptData {
		rootCAs := x509.NewCertPool()
		ok := rootCAs.AppendCertsFromPEM(caCertInBytes)
		if !ok || rootCAs == nil {
			log.Warnf("updateSecurityConfig: error appending certificate(s): %v", ok)
			return fmt.Errorf("error obtaining certificate(s)")
		}
		currSecurityConfig.rootCAs = rootCAs
	}

	// force reconnect cached gocbcore.Agents and DCPAgents
	statsAgentsMap.reconfigureSecurityForAgents(encryptData, FetchRootCAs)
	dcpAgentMap.reconfigureSecurityForAgents(encryptData, FetchRootCAs)

	return nil
}

func FetchRootCAs() *x509.CertPool {
	var rootCAs *x509.CertPool
	currSecurityConfigMutex.RLock()
	if currSecurityConfig.encryptData {
		rootCAs = currSecurityConfig.rootCAs
	}
	currSecurityConfigMutex.RUnlock()
	return rootCAs
}
