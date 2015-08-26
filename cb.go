//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbgt

import (
	"net/url"

	"github.com/couchbase/cbauth"
)

type CbAuthHandler struct {
	Hostport string
}

func (ah *CbAuthHandler) GetSaslCredentials() (string, string, error) {
	u, p, err := cbauth.GetMemcachedServiceAuth(ah.Hostport)
	if err != nil {
		return "", "", err
	}
	return u, p, nil
}

func (ah *CbAuthHandler) GetCredentials() (string, string, error) {
	u, p, err := cbauth.GetHTTPServiceAuth(ah.Hostport)
	if err != nil {
		return "", "", err
	}
	return u, p, nil
}

func NewCbAuthHandler(s string) (*CbAuthHandler, error) {
	u, err := url.Parse(s)
	if err == nil {
		return &CbAuthHandler{Hostport: u.Host}, nil
	}
	return nil, err
}
