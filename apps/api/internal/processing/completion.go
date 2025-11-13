package processing

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/metadata"
)

const (
	defaultCompletionInterval   = 25 * time.Millisecond // Match simulator interval (was 1 minute)
	defaultCompletionLookback   = 30 * time.Second // Increased lookback window
	defaultSamplesPerSensor     = 3
	defaultZeroThreshold        = 5.0
	defaultMeasurementForStatus = "sensor_data"
)

// CompletionService watches sensor readings and marks lots complete when machines stay down.
type CompletionService struct {
	influx              *influxdb.Client
	repo                *metadata.Repository
	interval            time.Duration
	lookback            time.Duration
	samplesRequired     int
	zeroThreshold       float64
	measurement         string
}

// CompletionOption customises the detector.
type CompletionOption func(*CompletionService)

// WithInterval overrides the poll interval.
func WithInterval(d time.Duration) CompletionOption {
	return func(s *CompletionService) {
		if d > 0 {
			s.interval = d
		}
	}
}

// WithLookback changes the query lookback window.
func WithLookback(d time.Duration) CompletionOption {
	return func(s *CompletionService) {
		if d > 0 {
			s.lookback = d
		}
	}
}

// WithSamplesRequired configures the minimum consecutive down samples per sensor.
func WithSamplesRequired(count int) CompletionOption {
	return func(s *CompletionService) {
		if count > 0 {
			s.samplesRequired = count
		}
	}
}

// WithZeroThreshold defines the maximum value still considered "down".
func WithZeroThreshold(threshold float64) CompletionOption {
	return func(s *CompletionService) {
		if threshold >= 0 {
			s.zeroThreshold = threshold
		}
	}
}

// WithMeasurement allows overriding the measurement name queried in Influx.
func WithMeasurement(name string) CompletionOption {
	return func(s *CompletionService) {
		if strings.TrimSpace(name) != "" {
			s.measurement = name
		}
	}
}

// NewCompletionService constructs a detector with sensible defaults.
func NewCompletionService(client *influxdb.Client, repo *metadata.Repository, opts ...CompletionOption) *CompletionService {
	svc := &CompletionService{
		influx:          client,
		repo:            repo,
		interval:        defaultCompletionInterval,
		lookback:        defaultCompletionLookback,
		samplesRequired: defaultSamplesPerSensor,
		zeroThreshold:   defaultZeroThreshold,
		measurement:     defaultMeasurementForStatus,
	}
	for _, opt := range opts {
		opt(svc)
	}
	return svc
}

// Start begins the background polling loop.
func (s *CompletionService) Start(ctx context.Context) {
	if s.influx == nil || s.repo == nil {
		return
	}

	ticker := time.NewTicker(s.interval)
	go func() {
		defer ticker.Stop()
		log.Printf("lot completion service running; interval=%s lookback=%s", s.interval, s.lookback)
		for {
			select {
			case <-ctx.Done():
				log.Println("lot completion service stopped")
				return
			case <-ticker.C:
				s.checkLots(ctx)
			}
		}
	}()
}

func (s *CompletionService) checkLots(ctx context.Context) {
	log.Printf("[DEBUG] checkLots: starting lot completion check cycle...")
	lots, err := s.repo.ListActiveLots(ctx)
	if err != nil {
		log.Printf("[DEBUG] checkLots: list active lots failed: %v", err)
		return
	}
	if len(lots) == 0 {
		log.Printf("[DEBUG] checkLots: no active lots found, skipping check")
		return
	}

	log.Printf("[DEBUG] checkLots: found %d active lot(s) to check", len(lots))

	for i, lot := range lots {
		log.Printf("[DEBUG] checkLots: [%d/%d] checking lot=%s machine=%s status=%s for sensor-down", 
			i+1, len(lots), lot.LotNumber, lot.MachineName, lot.Status)

		// Fallback to original sensor-down based completion
		summary, done, evalErr := s.evaluateLot(ctx, lot)
		if evalErr != nil {
			log.Printf("[DEBUG] checkLots: lot=%s evaluateLot error: %v", lot.LotNumber, evalErr)
			continue
		}
		if !done || summary == nil {
			log.Printf("[DEBUG] checkLots: lot=%s sensor-down check returned done=%v (not ready for completion)", lot.LotNumber, done)
			continue
		}

		log.Printf("[DEBUG] checkLots: lot=%s all sensors DOWN, marking as completed via sensor-down logic", lot.LotNumber)
		if err := s.repo.MarkLotCompleted(ctx, lot.ID, *summary); err != nil {
			if !errorsIsNoRows(err) {
				log.Printf("[DEBUG] checkLots: lot=%s MarkLotCompleted (sensor-down) error: %v", lot.LotNumber, err)
			}
			continue
		}
		log.Printf("✅ lot completion: lot %s marked complete via sensor-down (machine=%s)", lot.LotNumber, lot.MachineName)
	}
	log.Printf("[DEBUG] checkLots: cycle completed, processed %d lot(s)", len(lots))
}

func (s *CompletionService) evaluateLot(ctx context.Context, lot metadata.Lot) (*metadata.LotSummary, bool, error) {
	limit := s.samplesRequired * 8
	if limit < s.samplesRequired {
		limit = s.samplesRequired
	}
	readings, err := s.influx.RecentSensorReadingsByMachine(ctx, s.measurement, lot.MachineName, s.lookback, limit)
	if err != nil {
		return nil, false, err
	}
	if len(readings) == 0 {
		return nil, false, nil
	}

	sensorWindows := make(map[string][]influxdb.SensorReading)
	for _, reading := range readings {
		window := sensorWindows[reading.SensorName]
		if len(window) >= s.samplesRequired {
			continue
		}
		sensorWindows[reading.SensorName] = append(window, reading)
	}
	if len(sensorWindows) == 0 {
		return nil, false, nil
	}

	summary := metadata.LotSummary{
		CompletedAt: readings[0].Time,
		MachineName: lot.MachineName,
	}
	sensorNames := make([]string, 0, len(sensorWindows))
	for name := range sensorWindows {
		sensorNames = append(sensorNames, name)
	}
	sort.Strings(sensorNames)

	allDown := true
	for _, name := range sensorNames {
		samples := sensorWindows[name]
		if len(samples) < s.samplesRequired {
			allDown = false
		}
		avg := averageValue(samples)
		latest := samples[0]
		if !isDownSample(latest, s.zeroThreshold) {
			allDown = false
		}
		for _, sample := range samples[1:] {
			if !isDownSample(sample, s.zeroThreshold) {
				allDown = false
				break
			}
		}
		summary.Sensors = append(summary.Sensors, metadata.SensorSnapshot{
			SensorName:    name,
			LatestStatus:  latest.Status,
			LatestValue:   latest.Value,
			AverageDown:   avg,
			ObservedCount: len(samples),
		})
	}

	if !allDown {
		return nil, false, nil
	}

	return &summary, true, nil
}

func averageValue(samples []influxdb.SensorReading) float64 {
	if len(samples) == 0 {
		return 0
	}
	sum := 0.0
	for _, sample := range samples {
		sum += sample.Value
	}
	return sum / float64(len(samples))
}

func isDownSample(sample influxdb.SensorReading, threshold float64) bool {
	if !strings.EqualFold(sample.Status, "down") {
		return false
	}
	return sample.Value <= threshold
}

func errorsIsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
