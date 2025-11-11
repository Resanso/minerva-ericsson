package simulation

import (
	"log"
	"os"
	"time"
)

const intervalEnvKey = "SIMULATION_INTERVAL"

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
