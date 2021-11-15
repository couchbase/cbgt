//  Copyright 2021-Present Couchbase, Inc.
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
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

var HttpTransportDialContextTimeout = 30 * time.Second   // Go's default is 30 secs.
var HttpTransportDialContextKeepAlive = 30 * time.Second // Go's default is 30 secs.
var HttpTransportMaxIdleConns = 300                      // Go's default is 100 (0 means no limit).
var HttpTransportMaxIdleConnsPerHost = 100               // Go's default is 2.
var HttpTransportIdleConnTimeout = 90 * time.Second      // Go's default is 90 secs.
var HttpTransportTLSHandshakeTimeout = 10 * time.Second  // Go's default is 10 secs.
var HttpTransportExpectContinueTimeout = 1 * time.Second // Go's default is 1 secs.

var httpClientM sync.RWMutex
var httpClient = http.DefaultClient

func RegisterHttpClient() {
	RegisterConfigRefreshCallback("cbgt/httpClient", updateHttpClient)
	updateHttpClient(AuthChange_certificates)
}

func HttpClient() *http.Client {
	httpClientM.RLock()
	client := httpClient
	httpClientM.RUnlock()
	return client
}

func updateHttpClient(status int) error {
	if status&AuthChange_certificates != 0 {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   HttpTransportDialContextTimeout,
				KeepAlive: HttpTransportDialContextKeepAlive,
			}).DialContext,
			MaxIdleConns:          HttpTransportMaxIdleConns,
			MaxIdleConnsPerHost:   HttpTransportMaxIdleConnsPerHost,
			IdleConnTimeout:       HttpTransportIdleConnTimeout,
			TLSHandshakeTimeout:   HttpTransportTLSHandshakeTimeout,
			ExpectContinueTimeout: HttpTransportExpectContinueTimeout,
			TLSClientConfig:       &tls.Config{},
		}

		ss := GetSecuritySetting()
		rootCAs := x509.NewCertPool()
		ok := rootCAs.AppendCertsFromPEM(ss.CertInBytes)
		if ok {
			transport.TLSClientConfig.RootCAs = rootCAs
			_ = http2.ConfigureTransport(transport)
		} else {
			transport.TLSClientConfig.InsecureSkipVerify = true
		}

		client := &http.Client{
			Transport: transport,
		}

		httpClientM.Lock()
		httpClient = client
		httpClientM.Unlock()
	}

	return nil
}
