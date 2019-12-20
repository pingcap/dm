package config

import (
	"encoding/json"
	"time"
)

// Backoff related constants
var (
	DefaultCheckInterval           = 5 * time.Second
	DefaultBackoffRollback         = 5 * time.Minute
	DefaultBackoffMin              = 1 * time.Second
	DefaultBackoffMax              = 5 * time.Minute
	DefaultBackoffJitter           = true
	DefaultBackoffFactor   float64 = 2
)

// Duration is used to hold a time.Duration field
type Duration struct {
	time.Duration
}

// MarshalText hacks to satisfy the encoding.TextMarshaler interface
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalText hacks to satisfy the encoding.TextUnmarshaler interface
func (d Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalJSON hacks to satisfy the json.Marshaler interface
func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Duration string `json:"Duration"`
	}{
		d.Duration.String(),
	})
}

// CheckerConfig is configuration used for TaskStatusChecker
type CheckerConfig struct {
	CheckEnable     bool     `toml:"check-enable" json:"check-enable"`
	BackoffRollback Duration `toml:"backoff-rollback" json:"backoff-rollback"`
	BackoffMax      Duration `toml:"backoff-max" json:"backoff-max"`
	// unexpose config
	CheckInterval Duration `json:"-"`
	BackoffMin    Duration `json:"-"`
	BackoffJitter bool     `json:"-"`
	BackoffFactor float64  `json:"-"`
}

// Adjust sets default value for field: CheckInterval/BackoffMin/BackoffJitter/BackoffFactor
func (cc *CheckerConfig) Adjust() {
	cc.CheckInterval = Duration{Duration: DefaultCheckInterval}
	cc.BackoffMin = Duration{Duration: DefaultBackoffMin}
	cc.BackoffJitter = DefaultBackoffJitter
	cc.BackoffFactor = DefaultBackoffFactor
}
