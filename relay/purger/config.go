package purger

// Config is the configuration for Purger
type Config struct {
	Interval    int64 `toml:"interval" json:"interval"`         // check whether need to purge at this @Interval (seconds)
	Expires     int64 `toml:"expires" json:"expires"`           // if file's modified time is older than @Expires (hours), then it can be purged
	RemainSpace int64 `toml:"remain-space" json:"remain-space"` // if remain space in @RelayBaseDir less than @RemainSpace (GB), then it can be purged
}
