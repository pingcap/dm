package config

// Security config
type Security struct {
	SSLCA         string   `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert       string   `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey        string   `toml:"ssl-key" json:"ssl-key"`
	CertAllowedCN []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`
}
