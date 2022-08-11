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
	"io"
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

type HTTPClient interface {
	Get(string) (*http.Response, error)
	Post(string, string, io.Reader) (*http.Response, error)
	Do(*http.Request) (*http.Response, error)
}

// A wrapper over the HTTP client.
type WrapperHTTPClient struct {
	Client *http.Client
}

var httpClient = &WrapperHTTPClient{
	Client: http.DefaultClient,
}

func (w *WrapperHTTPClient) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return w.Do(req)
}

func (w *WrapperHTTPClient) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)

	return w.Do(req)
}

var UserAgentStr = "CB-SearchService"

func (w *WrapperHTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", UserAgentStr)
	return w.Client.Do(req)
}

func RegisterHttpClient() {
	RegisterConfigRefreshCallback("cbgt/httpClient", updateHttpClient)
	updateHttpClient(AuthChange_certificates | AuthChange_clientCertificates)
}

func HttpClient() HTTPClient {
	httpClientM.RLock()
	client := httpClient
	httpClientM.RUnlock()
	return client
}

func updateHttpClient(status int) error {
	if status&AuthChange_certificates != 0 ||
		status&AuthChange_clientCertificates != 0 {
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
		ok := rootCAs.AppendCertsFromPEM(ss.CACertInBytes)
		if ok {
			transport.TLSClientConfig.RootCAs = rootCAs
			transport.TLSClientConfig.Certificates = []tls.Certificate{ss.ClientCertificate}
			_ = http2.ConfigureTransport(transport)
		} else {
			transport.TLSClientConfig.InsecureSkipVerify = true
		}

		client := &http.Client{
			Transport: transport,
		}

		httpClientM.Lock()
		httpClient.Client = client
		httpClientM.Unlock()
	}

	return nil
}
