package influxdb

import (
	"context"
	"fmt"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
)

// Config maps the connection details required to reach InfluxDB.
type Config struct {
	URL     string
	Token   string
	Org     string
	Bucket  string
	Timeout time.Duration
}

// FromEnv loads configuration values from environment variables.
// INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, and INFLUX_BUCKET are required.
// INFLUX_TIMEOUT is optional and defaults to 5s when not provided.
func FromEnv() (Config, error) {
	cfg := Config{
		URL:    os.Getenv("INFLUX_URL"),
		Token:  os.Getenv("INFLUX_TOKEN"),
		Org:    os.Getenv("INFLUX_ORG"),
		Bucket: os.Getenv("INFLUX_BUCKET"),
	}

	if cfg.URL == "" || cfg.Token == "" || cfg.Org == "" || cfg.Bucket == "" {
		return Config{}, fmt.Errorf("missing InfluxDB configuration, ensure INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, and INFLUX_BUCKET are set")
	}

	timeout := os.Getenv("INFLUX_TIMEOUT")
	switch {
	case timeout == "":
		cfg.Timeout = 5 * time.Second
	default:
		dur, err := time.ParseDuration(timeout)
		if err != nil {
			return Config{}, fmt.Errorf("invalid INFLUX_TIMEOUT: %w", err)
		}
		cfg.Timeout = dur
	}

	return cfg, nil
}

// Client wraps the InfluxDB client with project-specific defaults.
type Client struct {
	cfg    Config
	client influxdb2.Client
}

// New establishes a new InfluxDB client based on the provided configuration.
// A ping is issued to ensure the connection is healthy before returning.
func New(ctx context.Context, cfg Config) (*Client, error) {
	client := influxdb2.NewClient(cfg.URL, cfg.Token)

	ctxPing := ctx
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctxPing, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}

	ok, err := client.Ping(ctxPing)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ping InfluxDB: %w", err)
	}
	if !ok {
		client.Close()
		return nil, fmt.Errorf("influxdb ping failed")
	}

	return &Client{cfg: cfg, client: client}, nil
}

// WriteAPI returns the blocking write API bound to the configured org and bucket.
func (c *Client) WriteAPI() api.WriteAPIBlocking {
	return c.client.WriteAPIBlocking(c.cfg.Org, c.cfg.Bucket)
}

// QueryAPI returns the query API bound to the configured org.
func (c *Client) QueryAPI() api.QueryAPI {
	return c.client.QueryAPI(c.cfg.Org)
}

// Ping checks the InfluxDB availability using the wrapped client.
func (c *Client) Ping(ctx context.Context) error {
	ok, err := c.client.Ping(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("influxdb ping failed")
	}
	return nil
}

// Close releases resources held by the underlying client.
func (c *Client) Close() {
	c.client.Close()
}
