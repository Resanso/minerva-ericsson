// /apps/api/main.go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/joho/godotenv"

	influx "github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/server"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/simulation"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env file not loaded: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := influx.FromEnv()
	if err != nil {
		log.Fatalf("influx config error: %v", err)
	}

	client, err := influx.New(ctx, cfg)
	if err != nil {
		log.Fatalf("influx connection error: %v", err)
	}
	defer client.Close()

	interval := simulation.IntervalFromEnv()
	simulator := simulation.New(client.WriteAPI(), simulation.DefaultSensors(), simulation.WithInterval(interval))
	simulator.Start(ctx)

	router := server.NewRouter(server.Dependencies{
		Simulator: simulator,
		Influx:    client,
	})

	fmt.Println("Starting Go Gin server on :8080...")
	if err := router.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
