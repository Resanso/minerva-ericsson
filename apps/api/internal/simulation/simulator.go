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
	defaultInterval        = 1 * time.Second
	measurementName        = "sensor_data"
	rebindCoefficient      = 0.1
	downCoefficient        = 0.25
	downNoiseScale         = 0.1
	defaultDownRatio       = 0.0
	minDownFraction        = 0.02
	startupInitialRatio    = 0.2
	startupRampCoefficient = 0.4
	startupNoiseScale      = 0.05
	shutdownCoefficient    = 0.35
	shutdownNoiseScale     = 0.05
)

type sensorState int

const (
	stateStartup sensorState = iota
	stateRunning
	stateShuttingDown
	stateDown
)

// CycleListener observes when the simulator finishes iterating across all machines once.
type CycleListener interface {
	OnCycleComplete(ctx context.Context, completedAt time.Time, lastMachine string)
}

type durationRange struct {
	min int
	max int
}

var (
	defaultStartupRange  = durationRange{min: 3, max: 6}
	defaultRunRange      = durationRange{min: 6, max: 24}
	defaultShutdownRange = durationRange{min: 3, max: 6}
	defaultDownRange     = durationRange{min: 6, max: 14}
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
	startupRange   durationRange
	runRange       durationRange
	shutdownRange  durationRange
	downRange      durationRange
	downTarget     float64
}

// Simulator generates time-series data for configured sensors.
type Simulator struct {
	writer            api.WriteAPIBlocking
	sensors           []*Sensor
	machineSensors    map[string][]*Sensor
	machineOrder      []string
	machineIndex      int
	machineIteration  int
	machineIterations int
	enabled           bool
	cycleListeners    []CycleListener
	mu                sync.RWMutex
	rng               *rand.Rand
	interval          time.Duration
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

// WithMachineIterations overrides how many ticks a machine should remain active
// before advancing to the next machine in the sequence.
func WithMachineIterations(iterations int) Option {
	return func(s *Simulator) {
		if iterations > 0 {
			s.machineIterations = iterations
		}
	}
}

// New creates a new Simulator.
func New(writer api.WriteAPIBlocking, sensors []*Sensor, opts ...Option) *Simulator {
	sim := &Simulator{
		writer:            writer,
		sensors:           sensors,
		machineSensors:    make(map[string][]*Sensor),
		machineIterations: MachineIterationsFromEnv(),
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		interval:          defaultInterval,
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
	if !s.enabled || len(s.machineOrder) == 0 {
		s.mu.Unlock()
		return
	}

	currentMachine := s.machineOrder[s.machineIndex]
	activeSensors := s.machineSensors[currentMachine]
	readings := make([]Sensor, len(activeSensors))
	for i, sensor := range activeSensors {
		value := s.nextValue(sensor)
		readings[i] = Sensor{
			MachineName:  sensor.MachineName,
			SensorName:   sensor.SensorName,
			CurrentValue: value,
			Status:       sensor.Status,
		}
	}

	cycleComplete := false
	lastMachine := currentMachine
	s.machineIteration++
	if s.machineIteration >= s.machineIterations {
		s.machineIteration = 0
		s.machineIndex = (s.machineIndex + 1) % len(s.machineOrder)
		if s.machineIndex == 0 {
			cycleComplete = true
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

	if cycleComplete {
		s.notifyCycleComplete(ctx, ts, lastMachine)
	}
}

func (s *Simulator) nextValue(sensor *Sensor) float64 {
	if sensor.ticksRemaining <= 0 {
		switch sensor.state {
		case stateStartup:
			s.enterState(sensor, stateRunning)
		case stateRunning:
			s.enterState(sensor, stateShuttingDown)
		case stateShuttingDown:
			s.enterState(sensor, stateDown)
		case stateDown:
			s.enterState(sensor, stateStartup)
		}
	}

	sensor.ticksRemaining--

	switch sensor.state {
	case stateStartup:
		if sensor.Baseline > 0 {
			sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * startupRampCoefficient
		}
		sensor.CurrentValue += (s.rng.Float64() - 0.5) * sensor.Drift * startupNoiseScale
	case stateRunning:
		change := (s.rng.Float64() - 0.5) * sensor.Drift
		sensor.CurrentValue += change
		sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * rebindCoefficient
	case stateShuttingDown:
		sensor.CurrentValue += (sensor.downTarget - sensor.CurrentValue) * shutdownCoefficient
		sensor.CurrentValue += (s.rng.Float64() - 0.5) * sensor.Drift * shutdownNoiseScale
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
	case stateStartup:
		sensor.Status = "starting"
		sensor.ticksRemaining = s.randomTicks(sensor.startupRange)
		if sensor.Baseline > 0 {
			base := sensor.Baseline * startupInitialRatio
			noise := (s.rng.Float64() - 0.5) * sensor.Drift * startupNoiseScale
			sensor.CurrentValue = base + noise
			if sensor.CurrentValue < 0 {
				sensor.CurrentValue = 0
			}
			if sensor.CurrentValue > sensor.Baseline {
				sensor.CurrentValue = sensor.Baseline
			}
		}
	case stateRunning:
		sensor.Status = "running"
		sensor.ticksRemaining = s.randomTicks(sensor.runRange)
		if sensor.Baseline > 0 && sensor.CurrentValue < sensor.Baseline*0.8 {
			sensor.CurrentValue += (sensor.Baseline - sensor.CurrentValue) * 0.3
		}
	case stateShuttingDown:
		sensor.Status = "shutting_down"
		sensor.ticksRemaining = s.randomTicks(sensor.shutdownRange)
	case stateDown:
		sensor.Status = "down"
		sensor.ticksRemaining = s.randomTicks(sensor.downRange)
		if sensor.CurrentValue > sensor.downTarget {
			sensor.CurrentValue = sensor.downTarget
		}
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
	s.machineSensors = make(map[string][]*Sensor)
	s.machineOrder = s.machineOrder[:0]
	if s.machineIterations <= 0 {
		s.machineIterations = 1
	}
	s.machineIndex = 0
	s.machineIteration = 0

	for _, sensor := range s.sensors {
		if sensor.startupRange.min == 0 && sensor.startupRange.max == 0 {
			sensor.startupRange = defaultStartupRange
		}
		if sensor.runRange.min == 0 && sensor.runRange.max == 0 {
			sensor.runRange = defaultRunRange
		}
		if sensor.shutdownRange.min == 0 && sensor.shutdownRange.max == 0 {
			sensor.shutdownRange = defaultShutdownRange
		}
		if sensor.downRange.min == 0 && sensor.downRange.max == 0 {
			sensor.downRange = defaultDownRange
		}
		if sensor.downTarget == 0 && sensor.Baseline > 0 {
			sensor.downTarget = sensor.Baseline * defaultDownRatio
		}

		s.enterState(sensor, stateStartup)

		if _, exists := s.machineSensors[sensor.MachineName]; !exists {
			s.machineOrder = append(s.machineOrder, sensor.MachineName)
		}
		s.machineSensors[sensor.MachineName] = append(s.machineSensors[sensor.MachineName], sensor)
	}
}

// Enable activates the simulator and resets sensor state.
func (s *Simulator) Enable() {
	s.mu.Lock()
	if s.enabled {
		s.mu.Unlock()
		return
	}
	s.initializeSensors()
	s.enabled = true
	machines := len(s.machineOrder)
	iters := s.machineIterations
	s.mu.Unlock()
	log.Printf("sensor simulator enabled; machines=%d iterationsPerMachine=%d", machines, iters)
}

// Disable pauses all sensor generation.
func (s *Simulator) Disable() {
	s.mu.Lock()
	if !s.enabled {
		s.mu.Unlock()
		return
	}
	s.enabled = false
	s.machineIndex = 0
	s.machineIteration = 0
	s.mu.Unlock()
	log.Println("sensor simulator disabled")
}

// Enabled reports whether the simulator is currently generating data.
func (s *Simulator) Enabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.enabled
}

// RegisterCycleListener subscribes to cycle completion events.
func (s *Simulator) RegisterCycleListener(listener CycleListener) {
	if listener == nil {
		return
	}
	s.mu.Lock()
	s.cycleListeners = append(s.cycleListeners, listener)
	s.mu.Unlock()
}

func (s *Simulator) notifyCycleComplete(ctx context.Context, ts time.Time, lastMachine string) {
	s.mu.RLock()
	listeners := append([]CycleListener(nil), s.cycleListeners...)
	s.mu.RUnlock()
	for _, listener := range listeners {
		listener.OnCycleComplete(ctx, ts, lastMachine)
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
	initialValue := baseline * startupInitialRatio
	initialValue += (rng.Float64() - 0.5) * initialSpread
	if initialValue < 0 {
		initialValue = 0
	}
	if baseline > 0 && initialValue > baseline {
		initialValue = baseline
	}
	return &Sensor{
		MachineName:   machine,
		SensorName:    sensor,
		Baseline:      baseline,
		Drift:         drift,
		CurrentValue:  initialValue,
		startupRange:  defaultStartupRange,
		runRange:      defaultRunRange,
		shutdownRange: defaultShutdownRange,
		downRange:     defaultDownRange,
		downTarget:    downTarget,
	}
}

// MeasurementName returns the measurement identifier used for simulated sensor writes.
func MeasurementName() string {
	return measurementName
}
