package simulation

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	api "github.com/influxdata/influxdb-client-go/v2/api"
)

const (
	defaultInterval   = 1 * time.Minute
	measurementName   = "sensor_data"
	rebindCoefficient = 0.1
)

// Sensor describes a simulated sensor configuration.
type Sensor struct {
	MachineName  string  `json:"machineName"`
	SensorName   string  `json:"sensorName"`
	CurrentValue float64 `json:"currentValue"`
	Baseline     float64 `json:"-"`
	Drift        float64 `json:"-"`
}

// Simulator generates time-series data for configured sensors.
type Simulator struct {
	writer   api.WriteAPIBlocking
	sensors  []*Sensor
	mu       sync.RWMutex
	rng      *rand.Rand
	interval time.Duration
}

// Option customizes Simulator creation.
type Option func(*Simulator)

// WithInterval overrides the default generation interval.
func WithInterval(interval time.Duration) Option {
	return func(s *Simulator) {
		if interval > 0 {
			s.interval = interval
		}
	}
}

// New creates a new Simulator.
func New(writer api.WriteAPIBlocking, sensors []*Sensor, opts ...Option) *Simulator {
	sim := &Simulator{
		writer:   writer,
		sensors:  sensors,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		interval: defaultInterval,
	}
	for _, opt := range opts {
		opt(sim)
	}
	return sim
}

// Start begins periodic data generation until ctx cancels.
func (s *Simulator) Start(ctx context.Context) {
	log.Printf("sensor simulator running; interval=%s sensors=%d", s.interval, len(s.sensors))
	ticker := time.NewTicker(s.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("sensor simulator stopped")
				return
			case ts := <-ticker.C:
				s.tick(ctx, ts)
			}
		}
	}()
}

func (s *Simulator) tick(ctx context.Context, ts time.Time) {
	s.mu.Lock()
	readings := make([]Sensor, len(s.sensors))
	for i, sensor := range s.sensors {
		value := s.nextValue(sensor)
		readings[i] = Sensor{
			MachineName:  sensor.MachineName,
			SensorName:   sensor.SensorName,
			CurrentValue: value,
		}
	}
	s.mu.Unlock()

	for _, reading := range readings {
		point := influxdb2.NewPoint(
			measurementName,
			map[string]string{
				"machine_name": reading.MachineName,
				"sensor_name":  reading.SensorName,
			},
			map[string]interface{}{
				"value": reading.CurrentValue,
			},
			ts,
		)
		if err := s.writer.WritePoint(ctx, point); err != nil {
			log.Printf("write sensor data failed: %v", err)
			continue
		}
		log.Printf("sensor simulated: machine=%s sensor=%s value=%.2f", reading.MachineName, reading.SensorName, reading.CurrentValue)
	}
}

func (s *Simulator) nextValue(sensor *Sensor) float64 {
	change := (s.rng.Float64() - 0.5) * sensor.Drift
	sensor.CurrentValue += change
	sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * rebindCoefficient
	return sensor.CurrentValue
}

// Snapshot returns a copy of sensors preserving current values.
func (s *Simulator) Snapshot() []Sensor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make([]Sensor, len(s.sensors))
	for i, sensor := range s.sensors {
		snapshot[i] = *sensor
	}
	return snapshot
}

// Interval returns the configured simulation interval.
func (s *Simulator) Interval() time.Duration {
	return s.interval
}

// NewSensor helper constructs a Sensor with a randomized start.
func NewSensor(machine, sensor string, baseline, drift, initialSpread float64) *Sensor {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Sensor{
		MachineName:  machine,
		SensorName:   sensor,
		Baseline:     baseline,
		Drift:        drift,
		CurrentValue: baseline + (rng.Float64()-0.5)*initialSpread,
	}
}
