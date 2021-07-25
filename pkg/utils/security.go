package utils

import (
	"crypto/tls"
	"crypto/x509"
	"strings"

	"github.com/pingcap/errors"
)

// ToTLSConfigWithVerifyByRawbytes constructs a `*tls.Config` from the CA, certification and key bytes
// and add verify for CN.
func ToTLSConfigWithVerifyByRawbytes(caData, certData, keyData []byte, verifyCN []string) (*tls.Config, error) {
	// Generate a key pair from your pem-encoded cert and key ([]byte).
	cert, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return nil, errors.New("failed to generate cert")
	}
	certificates := []tls.Certificate{cert}

	// Create a certificate pool from CA
	certPool := x509.NewCertPool()

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(caData) {
		return nil, errors.New("failed to append ca certs")
	}

	tlsCfg := &tls.Config{
		Certificates: certificates,
		RootCAs:      certPool,
		ClientCAs:    certPool,
		NextProtos:   []string{"h2", "http/1.1"}, // specify `h2` to let Go use HTTP/2.
	}

	if len(verifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range verifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}

		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert

		tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			cns := make([]string, 0, len(verifiedChains))
			for _, chains := range verifiedChains {
				for _, chain := range chains {
					cns = append(cns, chain.Subject.CommonName)
					if _, match := checkCN[chain.Subject.CommonName]; match {
						return nil
					}
				}
			}
			return errors.Errorf("client certificate authentication failed. The Common Name from the client certificate %v was not found in the configuration cluster-verify-cn with value: %s", cns, verifyCN)
		}
	}

	return tlsCfg, nil
}
