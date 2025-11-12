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
	downCoefficient   = 0.25
	downNoiseScale    = 0.1
	defaultDownRatio  = 0.0
	minDownFraction   = 0.02
)

type sensorState int

const (
	stateRunning sensorState = iota
	stateDown
)

type durationRange struct {
	min int
	max int
}

var (
	defaultRunRange  = durationRange{min: 6, max: 24}
	defaultDownRange = durationRange{min: 6, max: 14}
)

// Sensor describes a simulated sensor configuration with downtime behaviour.
type Sensor struct {
	MachineName  string  `json:"machineName"`
	SensorName   string  `json:"sensorName"`
	CurrentValue float64 `json:"currentValue"`
	Status       string  `json:"status"`

	Baseline float64 `json:"-"`
	Drift    float64 `json:"-"`

	state          sensorState
	ticksRemaining int
	runRange       durationRange
	downRange      durationRange
	downTarget     float64
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
	sim.initializeSensors()
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
			Status:       sensor.Status,
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
		log.Printf("sensor simulated: machine=%s sensor=%s status=%s value=%.2f", reading.MachineName, reading.SensorName, reading.Status, reading.CurrentValue)
	}
}

func (s *Simulator) nextValue(sensor *Sensor) float64 {
	if sensor.ticksRemaining <= 0 {
		if sensor.state == stateRunning {
			s.enterState(sensor, stateDown)
		} else {
			s.enterState(sensor, stateRunning)
			sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * 0.2
		}
	}

	sensor.ticksRemaining--

	switch sensor.state {
	case stateRunning:
		change := (s.rng.Float64() - 0.5) * sensor.Drift
		sensor.CurrentValue += change
		sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * rebindCoefficient
	case stateDown:
		sensor.CurrentValue += (sensor.downTarget - sensor.CurrentValue) * downCoefficient
		sensor.CurrentValue += (s.rng.Float64() - 0.5) * sensor.Drift * downNoiseScale
		if sensor.CurrentValue < sensor.downTarget {
			sensor.CurrentValue = sensor.downTarget
		}
		if sensor.downTarget == 0 && sensor.Baseline > 0 && sensor.CurrentValue < sensor.Baseline*minDownFraction {
			sensor.CurrentValue = 0
		}
	}

	if sensor.CurrentValue < 0 {
		sensor.CurrentValue = 0
	}
	return sensor.CurrentValue
}

func (s *Simulator) enterState(sensor *Sensor, newState sensorState) {
	sensor.state = newState
	switch newState {
	case stateRunning:
		sensor.Status = "running"
		sensor.ticksRemaining = s.randomTicks(sensor.runRange)
	case stateDown:
		sensor.Status = "down"
		sensor.ticksRemaining = s.randomTicks(sensor.downRange)
	}
}

func (s *Simulator) randomTicks(r durationRange) int {
	min := r.min
	if min < 1 {
		min = 1
	}
	max := r.max
	if max < min {
		max = min
	}
	span := max - min + 1
	if span <= 0 {
		span = 1
	}
	return min + s.rng.Intn(span)
}

func (s *Simulator) initializeSensors() {
	for _, sensor := range s.sensors {
		if sensor.runRange.min == 0 && sensor.runRange.max == 0 {
			sensor.runRange = defaultRunRange
		}
		if sensor.downRange.min == 0 && sensor.downRange.max == 0 {
			sensor.downRange = defaultDownRange
		}
		if sensor.downTarget == 0 && sensor.Baseline > 0 {
			sensor.downTarget = sensor.Baseline * defaultDownRatio
		}
		s.enterState(sensor, stateRunning)
	}
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
	downTarget := baseline * defaultDownRatio
	if downTarget < 0 {
		downTarget = 0
	}
	return &Sensor{
		MachineName:  machine,
		SensorName:   sensor,
		Baseline:     baseline,
		Drift:        drift,
		CurrentValue: baseline + (rng.Float64()-0.5)*initialSpread,
		runRange:     defaultRunRange,
		downRange:    defaultDownRange,
		downTarget:   downTarget,
	}
}

// MeasurementName returns the measurement identifier used for simulated sensor writes.
func MeasurementName() string {
	return measurementName
}
