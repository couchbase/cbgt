//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbgt

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/youmark/pkcs8"
)

// -----------------------------------------------------------------------------

// The following two APIs are modified versions of golang's crypto/tls APIs to
// accommodate PKCS#8 encrypted private keys ..
//  - https://pkg.go.dev/crypto/tls#LoadX509KeyPair
//  - https://pkg.go.dev/crypto/tls#X509KeyPair

func LoadX509KeyPair(certFile, keyFile string,
	privateKeyPassphrase []byte) (cert tls.Certificate, err error) {
	if len(certFile) == 0 || len(keyFile) == 0 {
		err = fmt.Errorf("LoadX509KeyPair, cert/key files not available")
		return
	}

	certPEMBlock, err := ioutil.ReadFile(certFile)
	if err != nil {
		err = fmt.Errorf("LoadX509KeyPair, error reading cert, %v", err)
		return
	}

	keyPEMBlock, err := ioutil.ReadFile(keyFile)
	if err != nil {
		err = fmt.Errorf("LoadX509KeyPair, error reading pkey, %v", err)
		return
	}

	return x509KeyPair(certPEMBlock, keyPEMBlock, privateKeyPassphrase)
}

func x509KeyPair(certPEMBlock, keyPEMBlock, privateKeyPassphrase []byte) (
	cert tls.Certificate, err error) {
	var certDERBlock, keyDERBlock *pem.Block

	for {
		certDERBlock, certPEMBlock = pem.Decode(certPEMBlock)
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" {
			cert.Certificate = append(cert.Certificate, certDERBlock.Bytes)
		}
	}

	if len(cert.Certificate) == 0 {
		err = fmt.Errorf("x509KeyPair, failed to obtain certificate PEM data")
		return
	}

	for {
		keyDERBlock, keyPEMBlock = pem.Decode(keyPEMBlock)
		if keyDERBlock == nil {
			err = fmt.Errorf("x509KeyPair, failed to parse key PEM data")
			return
		}
		if keyDERBlock.Type == "PRIVATE KEY" ||
			strings.HasSuffix(keyDERBlock.Type, "PRIVATE KEY") {
			break
		}
	}

	cert.PrivateKey, err = parsePrivateKey(keyDERBlock.Bytes, privateKeyPassphrase)
	if err != nil {
		err = fmt.Errorf("x509KeyPair, parsePrivateKey err: %v", err)
		return
	}

	// We don't need to parse the public key for TLS, but we so do anyway
	// to check that it looks sane and matches the private key.
	x509Cert, er := x509.ParseCertificate(cert.Certificate[0])
	if er != nil {
		err = fmt.Errorf("x509KeyPair, ParseCertificate err: %v", er)
		return
	}

	switch pub := x509Cert.PublicKey.(type) {
	case *rsa.PublicKey:
		priv, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			err = fmt.Errorf("x509KeyPair, (rsa) private key does not match public key type")
			return
		}
		if pub.N.Cmp(priv.N) != 0 {
			err = fmt.Errorf("x509KeyPair, (rsa) private key does not match public key")
			return
		}
	case *ecdsa.PublicKey:
		priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
		if !ok {
			err = fmt.Errorf("x509KeyPair, (ecdsa) private key does not match public key type")
			return
		}
		if pub.X.Cmp(priv.X) != 0 || pub.Y.Cmp(priv.Y) != 0 {
			err = fmt.Errorf("x509KeyPair, (ecdsa) private key does not match public key")
			return
		}
	case ed25519.PublicKey:
		priv, ok := cert.PrivateKey.(ed25519.PrivateKey)
		if !ok {
			err = fmt.Errorf("x509KeyPair, (ed25519) private key does not match public key type")
			return
		}
		if !bytes.Equal(priv.Public().(ed25519.PublicKey), pub) {
			err = fmt.Errorf("x509KeyPair, (ed25519) private key does not match public key")
			return
		}
	default:
		err = fmt.Errorf("x509KeyPair, unknown public key algorithm")
		return
	}

	return cert, nil
}

func parsePrivateKey(der, privateKeyPassphrase []byte) (crypto.PrivateKey, error) {
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}

	// youmark/pkcs8's ParsePKCS8PrivateKey(..) accepts a password to decode
	// encrypted PKCS#8 private keys
	if key, err := pkcs8.ParsePKCS8PrivateKey(der, privateKeyPassphrase); err == nil {
		switch key := key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey, ed25519.PrivateKey:
			return key, nil
		default:
			return nil, fmt.Errorf("parsePrivateKey, unknown private key type")
		}
	}

	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	return nil, fmt.Errorf("parsePrivateKey, failed to parse private key")
}
