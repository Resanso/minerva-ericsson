package metadata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// ErrMachineNameRequired is returned when an insert lacks a name.
var ErrMachineNameRequired = errors.New("machine name is required")

// Repository persists non time-series metadata in MySQL.
type Repository struct {
	db *sql.DB
}

// Machine represents a physical asset associated with the simulator.
type Machine struct {
	ID          int64     `json:"id"`
	MachineName string    `json:"machineName"`
	Location    string    `json:"location"`
	CreatedAt   time.Time `json:"createdAt"`
}

// CreateMachineInput is used for inserting a new machine row.
type CreateMachineInput struct {
	MachineName string
	Location    string
}

// NewRepository constructs a Repository with the provided sql.DB pool.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// EnsureSchema creates the required tables if they are missing.
func (r *Repository) EnsureSchema(ctx context.Context) error {
	if err := r.ensureMachinesTable(ctx); err != nil {
		return err
	}
	return r.ensureLotsTable(ctx)
}

func (r *Repository) ensureMachinesTable(ctx context.Context) error {
	const ddl = `CREATE TABLE IF NOT EXISTS machines (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		machine_name VARCHAR(255) NOT NULL,
		location VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := r.db.ExecContext(ctx, ddl)
	return err
}

func (r *Repository) ensureLotsTable(ctx context.Context) error {
	const ddl = `CREATE TABLE IF NOT EXISTS lots (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		lot_number VARCHAR(255) NOT NULL UNIQUE,
		machine_name VARCHAR(255) NOT NULL,
		active_machine_id VARCHAR(255) NULL,
		status VARCHAR(32) NOT NULL DEFAULT 'processing',
		started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		completed_at TIMESTAMP NULL,
		summary_json JSON NULL,
		averages_json JSON NULL,
		operation_hour VARCHAR(32) NULL,
		good_product INT NULL,
		defect_product INT NULL,
		conclusion TEXT NULL,
		is_conclusion BOOLEAN NOT NULL DEFAULT FALSE,
		INDEX idx_lots_status (status),
		INDEX idx_lots_machine (machine_name)
	)`
	if _, err := r.db.ExecContext(ctx, ddl); err != nil {
		return err
	}

	alterStatements := []string{
		`ALTER TABLE lots ADD COLUMN active_machine_id VARCHAR(255) NULL AFTER machine_name`,
		`ALTER TABLE lots ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER started_at`,
		`ALTER TABLE lots ADD COLUMN averages_json JSON NULL AFTER summary_json`,
		`ALTER TABLE lots ADD COLUMN operation_hour VARCHAR(32) NULL AFTER averages_json`,
		`ALTER TABLE lots ADD COLUMN good_product INT NULL AFTER operation_hour`,
		`ALTER TABLE lots ADD COLUMN defect_product INT NULL AFTER good_product`,
		`ALTER TABLE lots ADD COLUMN conclusion TEXT NULL AFTER defect_product`,
		`ALTER TABLE lots ADD COLUMN is_conclusion BOOLEAN NOT NULL DEFAULT FALSE AFTER conclusion`,
	}
	for _, stmt := range alterStatements {
		if _, err := r.db.ExecContext(ctx, stmt); err != nil {
			if isDuplicateColumnError(err) {
				continue
			}
			return err
		}
	}
	return nil
}

// Ping checks MySQL connectivity using the provided context.
func (r *Repository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

// ListMachines returns all machines ordered by creation time descending.
func (r *Repository) ListMachines(ctx context.Context) ([]Machine, error) {
	const query = `SELECT id, machine_name, location, created_at FROM machines ORDER BY created_at DESC`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var machines []Machine
	for rows.Next() {
		var m Machine
		if err := rows.Scan(&m.ID, &m.MachineName, &m.Location, &m.CreatedAt); err != nil {
			return nil, err
		}
		machines = append(machines, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return machines, nil
}

// CreateMachine inserts a machine row and returns the stored record.
func (r *Repository) CreateMachine(ctx context.Context, input CreateMachineInput) (Machine, error) {
	if strings.TrimSpace(input.MachineName) == "" {
		return Machine{}, ErrMachineNameRequired
	}

	const stmt = `INSERT INTO machines (machine_name, location) VALUES (?, ?)`
	res, err := r.db.ExecContext(ctx, stmt, input.MachineName, nullableString(input.Location))
	if err != nil {
		return Machine{}, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return Machine{}, err
	}

	const query = `SELECT id, machine_name, location, created_at FROM machines WHERE id = ?`
	var m Machine
	if err := r.db.QueryRowContext(ctx, query, id).Scan(&m.ID, &m.MachineName, &m.Location, &m.CreatedAt); err != nil {
		return Machine{}, err
	}
	return m, nil
}

func nullableString(val string) interface{} {
	if strings.TrimSpace(val) == "" {
		return nil
	}
	return val
}

func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate column name")
}
