package influxdb

import (
	"context"
	"fmt"
	"os"
	"strings"
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

// SensorReading represents a single measurement row returned from InfluxDB.
type SensorReading struct {
	Time        time.Time
	MachineName string
	SensorName  string
	Status      string
	Value       float64
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

// Config exposes the immutable client configuration.
func (c *Client) Config() Config {
	return c.cfg
}

// RecentSensorReadings fetches the newest sensor values within the provided lookback window.
func (c *Client) RecentSensorReadings(ctx context.Context, measurement string, lookback time.Duration, limit int) ([]SensorReading, error) {
	return c.recentSensorReadings(ctx, measurement, nil, lookback, limit)
}

// RecentSensorReadingsByMachine fetches filtered data for a specific machine tag.
func (c *Client) RecentSensorReadingsByMachine(ctx context.Context, measurement, machineName string, lookback time.Duration, limit int) ([]SensorReading, error) {
	if strings.TrimSpace(machineName) == "" {
		return nil, fmt.Errorf("machine name is required")
	}
	f := map[string]string{"machine_name": machineName}
	return c.recentSensorReadings(ctx, measurement, f, lookback, limit)
}

// RecentSensorReadingsByMachineAndSensor fetches filtered data for a specific machine and sensor.
func (c *Client) RecentSensorReadingsByMachineAndSensor(ctx context.Context, measurement, machineName, sensorName string, lookback time.Duration, limit int) ([]SensorReading, error) {
	if strings.TrimSpace(machineName) == "" {
		return nil, fmt.Errorf("machine name is required")
	}
	if strings.TrimSpace(sensorName) == "" {
		return nil, fmt.Errorf("sensor name is required")
	}
	f := map[string]string{
		"machine_name": machineName,
		"sensor_name":  sensorName,
	}
	return c.recentSensorReadings(ctx, measurement, f, lookback, limit)
}

func (c *Client) recentSensorReadings(ctx context.Context, measurement string, filters map[string]string, lookback time.Duration, limit int) ([]SensorReading, error) {
	if measurement == "" {
		return nil, fmt.Errorf("measurement is required")
	}
	if lookback <= 0 {
		lookback = time.Hour
	}

	flux := fmt.Sprintf(`from(bucket: %q)
|> range(start: -%s)
|> filter(fn: (r) => r["_measurement"] == %q)
|> filter(fn: (r) => r["_field"] == "value")`, c.cfg.Bucket, toFluxDuration(lookback), measurement)

	for key, value := range filters {
		flux = fmt.Sprintf("%s\n|> filter(fn: (r) => r[%q] == %s)", flux, key, fluxStringLiteral(value))
	}

	flux += "\n|> sort(columns: [\"_time\"], desc: true)"
	if limit > 0 {
		flux = fmt.Sprintf("%s\n|> limit(n:%d)", flux, limit)
	}

	result, err := c.QueryAPI().Query(ctx, flux)
	if err != nil {
		return nil, fmt.Errorf("query influx: %w", err)
	}
	defer result.Close()

	readings := make([]SensorReading, 0, max(limit, 0))
	for result.Next() {
		record := result.Record()
		value, ok := record.Value().(float64)
		if !ok {
			switch v := record.Value().(type) {
			case int64:
				value = float64(v)
			case uint64:
				value = float64(v)
			default:
				continue
			}
		}

		readings = append(readings, SensorReading{
			Time:        record.Time(),
			MachineName: stringify(record.ValueByKey("machine_name")),
			SensorName:  stringify(record.ValueByKey("sensor_name")),
			Status:      stringify(record.ValueByKey("status")),
			Value:       value,
		})
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("iterate influx result: %w", err)
	}

	return readings, nil
}

// SensorReadingsSince fetches sensor values recorded after the provided start timestamp.
func (c *Client) SensorReadingsSince(ctx context.Context, measurement string, start time.Time, filters map[string]string, limit int) ([]SensorReading, error) {
	if measurement == "" {
		return nil, fmt.Errorf("measurement is required")
	}
	if start.IsZero() {
		start = time.Now().Add(-time.Hour)
	}

	flux := fmt.Sprintf(`from(bucket: %q)
|> range(start: time(v: %q))
|> filter(fn: (r) => r["_measurement"] == %q)
|> filter(fn: (r) => r["_field"] == "value")`, c.cfg.Bucket, start.UTC().Format(time.RFC3339Nano), measurement)

	for key, value := range filters {
		flux = fmt.Sprintf("%s\n|> filter(fn: (r) => r[%q] == %s)", flux, key, fluxStringLiteral(value))
	}

	flux += "\n|> sort(columns: [\"_time\"])"
	if limit > 0 {
		flux = fmt.Sprintf("%s\n|> limit(n:%d)", flux, limit)
	}

	result, err := c.QueryAPI().Query(ctx, flux)
	if err != nil {
		return nil, fmt.Errorf("query influx: %w", err)
	}
	defer result.Close()

	readings := make([]SensorReading, 0, max(limit, 0))
	for result.Next() {
		record := result.Record()
		value, ok := record.Value().(float64)
		if !ok {
			switch v := record.Value().(type) {
			case int64:
				value = float64(v)
			case uint64:
				value = float64(v)
			default:
				continue
			}
		}

		readings = append(readings, SensorReading{
			Time:        record.Time(),
			MachineName: stringify(record.ValueByKey("machine_name")),
			SensorName:  stringify(record.ValueByKey("sensor_name")),
			Status:      stringify(record.ValueByKey("status")),
			Value:       value,
		})
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("iterate influx result: %w", err)
	}

	return readings, nil
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

func toFluxDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	d = d.Truncate(time.Second)
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	}
	if d%time.Second == 0 {
		return fmt.Sprintf("%ds", int64(d/time.Second))
	}
	return fmt.Sprintf("%dns", d.Nanoseconds())
}

func stringify(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func fluxStringLiteral(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return fmt.Sprintf("\"%s\"", s)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
