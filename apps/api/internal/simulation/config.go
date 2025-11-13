package simulation

import (
	"log"
	"os"
	"strconv"
	"time"
)

const (
	intervalEnvKey          = "SIMULATION_INTERVAL"
	machineIterationsEnvKey = "SIMULATION_MACHINE_ITERATIONS"
	defaultMachineIters     = 2
)

// IntervalFromEnv reads the environment variable and falls back to the default interval.
func IntervalFromEnv() time.Duration {
	return IntervalFromString(os.Getenv(intervalEnvKey))
}

// IntervalFromString parses a duration string with sensible fallback.
func IntervalFromString(raw string) time.Duration {
	if raw == "" {
		return defaultInterval
	}
	dur, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("invalid %s value %q: %v, using default %s", intervalEnvKey, raw, err, defaultInterval)
		return defaultInterval
	}
	if dur <= 0 {
		log.Printf("non-positive %s value %q, using default %s", intervalEnvKey, raw, defaultInterval)
		return defaultInterval
	}
	return dur
}

// MachineIterationsFromEnv reads the environment variable controlling how many
// ticks a machine remains active before advancing to the next one.
func MachineIterationsFromEnv() int {
	raw := os.Getenv(machineIterationsEnvKey)
	if raw == "" {
		return defaultMachineIters
	}
	iterations, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid %s value %q: %v, using default %d", machineIterationsEnvKey, raw, err, defaultMachineIters)
		return defaultMachineIters
	}
	if iterations <= 0 {
		log.Printf("non-positive %s value %q, using default %d", machineIterationsEnvKey, raw, defaultMachineIters)
		return defaultMachineIters
	}
	return iterations
}
