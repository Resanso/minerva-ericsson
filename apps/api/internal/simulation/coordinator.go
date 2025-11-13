package simulation

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/Resanso/minerva-ericsson/apps/api/internal/metadata"
)

const defaultCoordinatorPollInterval = 5 * time.Second

// Coordinator orchestrates the simulator based on product status in MySQL.
// It enables simulation when processing lots exist and marks them completed
// once every machine finishes a cycle.
type Coordinator struct {
	simulator    *Simulator
	repo         *metadata.Repository
	pollInterval time.Duration
}

// CoordinatorOption customises coordinator behaviour.
type CoordinatorOption func(*Coordinator)

// WithCoordinatorPollInterval overrides how frequently MySQL is polled for active lots.
func WithCoordinatorPollInterval(d time.Duration) CoordinatorOption {
	return func(c *Coordinator) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// NewCoordinator wires the simulator with the metadata repository to control lifecycle.
func NewCoordinator(sim *Simulator, repo *metadata.Repository, opts ...CoordinatorOption) *Coordinator {
	coord := &Coordinator{
		simulator:    sim,
		repo:         repo,
		pollInterval: defaultCoordinatorPollInterval,
	}
	for _, opt := range opts {
		opt(coord)
	}
	if coord.simulator != nil {
		coord.simulator.RegisterCycleListener(coord)
	}
	return coord
}

// Start begins background orchestration until the context is cancelled.
func (c *Coordinator) Start(ctx context.Context) {
	if c.simulator == nil || c.repo == nil {
		log.Printf("simulation coordinator inactive (simulator or repository missing)")
		return
	}

	if err := c.syncSimulation(ctx); err != nil {
		log.Printf("simulation coordinator initial sync error: %v", err)
	}

	go c.run(ctx)
}

func (c *Coordinator) run(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("simulation coordinator stopped")
			return
		case <-ticker.C:
			if err := c.syncSimulation(ctx); err != nil {
				log.Printf("simulation coordinator sync error: %v", err)
			}
		}
	}
}

func (c *Coordinator) syncSimulation(ctx context.Context) error {
	active, err := c.repo.HasActiveLots(ctx)
	if err != nil {
		return err
	}
	if active {
		c.simulator.Enable()
	} else {
		c.simulator.Disable()
	}
	return nil
}

// OnCycleComplete marks processing lots as completed when the simulator
// finishes a full machine cycle.
func (c *Coordinator) OnCycleComplete(ctx context.Context, completedAt time.Time, lastMachine string) {
	if c.repo == nil {
		return
	}

	lots, err := c.repo.ListActiveLots(ctx)
	if err != nil {
		log.Printf("simulation coordinator list active lots error: %v", err)
		return
	}
	if len(lots) == 0 {
		return
	}

	completionTime := completedAt.UTC()
	if completionTime.IsZero() {
		completionTime = time.Now().UTC()
	}

	for _, lot := range lots {
		summary := metadata.LotSummary{
			CompletedAt: completionTime,
			MachineName: lot.MachineName,
		}
		if err := c.repo.MarkLotCompleted(ctx, lot.ID, summary); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			log.Printf("simulation coordinator mark lot complete failed lot=%s: %v", lot.LotNumber, err)
			continue
		}
		log.Printf("simulation coordinator lot=%s marked completed (lastMachine=%s)", lot.LotNumber, lastMachine)
	}

	remaining, err := c.repo.HasActiveLots(ctx)
	if err != nil {
		log.Printf("simulation coordinator post-completion check error: %v", err)
		return
	}
	if !remaining {
		c.simulator.Disable()
	}
}
