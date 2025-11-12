// /apps/api/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/joho/godotenv"

	influx "github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/llm"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/metadata"
	mysqlclient "github.com/Resanso/minerva-ericsson/apps/api/internal/mysql"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/processing"
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

	var llmClient *llm.Client
	if llmCfg, err := llm.FromEnv(); err != nil {
		switch {
		case errors.Is(err, llm.ErrMissingAPIKey):
			log.Printf("warning: LLM client disabled: %v", err)
		default:
			log.Fatalf("llm config error: %v", err)
		}
	} else {
		llmClient, err = llm.New(ctx, llmCfg)
		if err != nil {
			log.Fatalf("llm client error: %v", err)
		}
		defer func() {
			if err := llmClient.Close(); err != nil {
				log.Printf("warning: llm client close error: %v", err)
			}
		}()
	}

	interval := simulation.IntervalFromEnv()
	simulator := simulation.New(client.WriteAPI(), simulation.DefaultSensors(), simulation.WithInterval(interval))
	simulator.Start(ctx)

	mysqlCfg, err := mysqlclient.FromEnv()
	if err != nil {
		log.Fatalf("mysql config error: %v", err)
	}

	sqlDB, err := mysqlclient.New(ctx, mysqlCfg)
	if err != nil {
		log.Fatalf("mysql connection error: %v", err)
	}
	defer sqlDB.Close()

	metadataRepo := metadata.NewRepository(sqlDB)
	if err := metadataRepo.EnsureSchema(ctx); err != nil {
		log.Fatalf("mysql ensure schema error: %v", err)
	}

	completionService := processing.NewCompletionService(client, metadataRepo)
	completionService.Start(ctx)

	router := server.NewRouter(server.Dependencies{
		Simulator: simulator,
		Influx:    client,
		Metadata:  metadataRepo,
		LLM:       llmClient,
	})

	fmt.Println("Starting Go Gin server on :8080...")
	if err := router.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
