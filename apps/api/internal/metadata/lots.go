package metadata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// LotStatus represents the current processing stage of a lot.
type LotStatus string

const (
	LotStatusProcessing LotStatus = "processing"
	LotStatusCompleted  LotStatus = "completed"
)

// Lot holds the persisted state for a manufacturing lot.
type Lot struct {
	ID              int64           `json:"id"`
	LotNumber       string          `json:"lotNumber"`
	MachineName     string          `json:"machineName"`
	Status          LotStatus       `json:"status"`
	StartedAt       time.Time       `json:"startedAt"`
	CompletedAt     sql.NullTime    `json:"completedAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
	ActiveMachineID *string         `json:"activeMachineId,omitempty"`
	Averages        json.RawMessage `json:"averages,omitempty"`
	OperationHour   *string         `json:"operationHour,omitempty"`
	GoodProduct     *int            `json:"goodProduct,omitempty"`
	DefectProduct   *int            `json:"defectProduct,omitempty"`
	Conclusion      *string         `json:"conclusion,omitempty"`
	SummaryJSON     json.RawMessage `json:"summary,omitempty"`
	IsConclusion    bool            `json:"isConclusion"`
}

// CreateLotInput captures the values required to register a lot.
type CreateLotInput struct {
	LotNumber   string
	MachineName string
}

// ProductInput holds manual product information stored alongside a lot.
type ProductInput struct {
	LotNumber       string
	MachineName     string
	ActiveMachineID *string
	Averages        json.RawMessage
	OperationHour   *string
	GoodProduct     *int
	DefectProduct   *int
	Conclusion      *string
	IsConclusion    *bool
}

// LotSummary stores aggregated sensor context when a lot completes.
type LotSummary struct {
	CompletedAt   time.Time        `json:"completedAt"`
	MachineName   string           `json:"machineName"`
	Sensors       []SensorSnapshot `json:"sensors"`
	GoodProduct   int              `json:"goodProduct,omitempty"`
	DefectProduct int              `json:"defectProduct,omitempty"`
	Conclusion    string           `json:"conclusion,omitempty"`
}

// SensorSnapshot summarises the latest readings per sensor.
type SensorSnapshot struct {
	SensorName    string  `json:"sensorName"`
	LatestStatus  string  `json:"latestStatus"`
	LatestValue   float64 `json:"latestValue"`
	AverageDown   float64 `json:"averageDown"`
	ObservedCount int     `json:"observedCount"`
}

// ProductData represents the product summary returned to API consumers.
type ProductData struct {
	Lot             string             `json:"lot"`
	Status          LotStatus          `json:"status"`
	ActiveMachineID string             `json:"activeMachineId"`
	Averages        map[string]float64 `json:"averages"`
	OperationHour   float64            `json:"operationHour"`
	GoodProduct     int                `json:"goodProduct"`
	DefectProduct   int                `json:"defectProduct"`
	Conclusion      string             `json:"conclusion,omitempty"`
	IsConclusion    bool               `json:"isConclusion"`
	UpdatedAt       time.Time          `json:"updatedAt"`
}

// ErrLotExists indicates the provided lot number already exists.
var ErrLotExists = errors.New("lot already exists")

// ErrLotNumberRequired indicates the lot identifier is missing.
var ErrLotNumberRequired = errors.New("lot number is required")

// ErrLotNotFound indicates a lookup failed to locate the requested lot.
var ErrLotNotFound = errors.New("lot not found")

func normalizeMachineName(lotNumber, machineName string) string {
	trimmed := strings.TrimSpace(machineName)
	if trimmed != "" {
		return trimmed
	}
	fallback := strings.TrimSpace(lotNumber)
	if fallback != "" {
		return fallback
	}
	return "auto-machine"
}

// CreateLot inserts a new lot marked as processing.
func (r *Repository) CreateLot(ctx context.Context, input CreateLotInput) (Lot, error) {
	lotNumber := strings.TrimSpace(input.LotNumber)
	if lotNumber == "" {
		return Lot{}, ErrLotNumberRequired
	}
	machineName := normalizeMachineName(lotNumber, input.MachineName)

	const stmt = `INSERT INTO lots (lot_number, machine_name, status) VALUES (?, ?, ?)`
	res, err := r.db.ExecContext(ctx, stmt, lotNumber, machineName, LotStatusProcessing)
	if err != nil {
		if isDuplicateEntry(err) {
			return Lot{}, ErrLotExists
		}
		return Lot{}, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return Lot{}, err
	}

	return r.GetLotByID(ctx, id)
}

// UpsertLotProduct stores manual product metadata for a lot, creating the lot when necessary.
func (r *Repository) UpsertLotProduct(ctx context.Context, input ProductInput) (ProductData, error) {
	lotNumber := strings.TrimSpace(input.LotNumber)
	if lotNumber == "" {
		return ProductData{}, ErrLotNumberRequired
	}

	lot, err := r.GetLotByNumber(ctx, lotNumber)
	switch {
	case err == nil:
		// existing lot found
	case errors.Is(err, ErrLotNotFound):
		machine := normalizeMachineName(lotNumber, input.MachineName)
		lot, err = r.CreateLot(ctx, CreateLotInput{LotNumber: lotNumber, MachineName: machine})
		if err != nil {
			return ProductData{}, err
		}
	default:
		return ProductData{}, err
	}

	activeMachine := toNullString(input.ActiveMachineID)
	averages := toNullRawMessage(input.Averages)
	opHour := toNullString(input.OperationHour)
	good := toNullInt(input.GoodProduct)
	defect := toNullInt(input.DefectProduct)
	conclusion := toNullString(input.Conclusion)

	const stmt = `UPDATE lots SET active_machine_id = ?, averages_json = ?, operation_hour = ?, good_product = ?, defect_product = ?, conclusion = ?, is_conclusion = COALESCE(?, is_conclusion), updated_at = NOW() WHERE id = ?`
	isConclusion := toNullBool(input.IsConclusion)
	if _, err := r.db.ExecContext(ctx, stmt, activeMachine, averages, opHour, good, defect, conclusion, isConclusion, lot.ID); err != nil {
		return ProductData{}, err
	}

	updated, err := r.GetLotByID(ctx, lot.ID)
	if err != nil {
		return ProductData{}, err
	}
	product, err := lotToProductData(updated, time.Now().UTC())
	if err != nil {
		return ProductData{}, err
	}
	return product, nil
}

// GetLotByID retrieves a single lot record by its identifier.
func (r *Repository) GetLotByID(ctx context.Context, id int64) (Lot, error) {
	const query = `SELECT id, lot_number, machine_name, status, started_at, completed_at, updated_at, summary_json, active_machine_id, averages_json, operation_hour, good_product, defect_product, conclusion, is_conclusion FROM lots WHERE id = ?`
	row := r.db.QueryRowContext(ctx, query, id)
	lot, err := scanLot(row)
	if err != nil {
		return Lot{}, mapLotError(err)
	}
	return lot, nil
}

// GetLotByNumber fetches a lot using its public identifier.
func (r *Repository) GetLotByNumber(ctx context.Context, lotNumber string) (Lot, error) {
	const query = `SELECT id, lot_number, machine_name, status, started_at, completed_at, updated_at, summary_json, active_machine_id, averages_json, operation_hour, good_product, defect_product, conclusion, is_conclusion FROM lots WHERE lot_number = ?`
	row := r.db.QueryRowContext(ctx, query, lotNumber)
	lot, err := scanLot(row)
	if err != nil {
		return Lot{}, mapLotError(err)
	}
	return lot, nil
}

// ListLots returns all lots ordered by start time desc.
func (r *Repository) ListLots(ctx context.Context) ([]Lot, error) {
	const query = `SELECT id, lot_number, machine_name, status, started_at, completed_at, updated_at, summary_json, active_machine_id, averages_json, operation_hour, good_product, defect_product, conclusion, is_conclusion FROM lots ORDER BY started_at DESC`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lots []Lot
	for rows.Next() {
		lot, err := scanLot(rows)
		if err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

// ListActiveLots returns lots that are not yet completed.
func (r *Repository) ListActiveLots(ctx context.Context) ([]Lot, error) {
	const query = `SELECT id, lot_number, machine_name, status, started_at, completed_at, updated_at, summary_json, active_machine_id, averages_json, operation_hour, good_product, defect_product, conclusion, is_conclusion FROM lots WHERE status = ? ORDER BY started_at`
	rows, err := r.db.QueryContext(ctx, query, LotStatusProcessing)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var lots []Lot
	for rows.Next() {
		lot, err := scanLot(rows)
		if err != nil {
			return nil, err
		}
		lots = append(lots, lot)
	}
	return lots, rows.Err()
}

// HasActiveLots reports whether any lots are currently in processing state.
func (r *Repository) HasActiveLots(ctx context.Context) (bool, error) {
	const query = `SELECT 1 FROM lots WHERE status = ? LIMIT 1`
	var flag int
	err := r.db.QueryRowContext(ctx, query, LotStatusProcessing).Scan(&flag)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

// MarkLotCompleted updates a lot as completed and stores the summary payload.
func (r *Repository) MarkLotCompleted(ctx context.Context, lotID int64, summary LotSummary) error {
	payload, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("marshal lot summary: %w", err)
	}

	const stmt = `UPDATE lots SET status = ?, completed_at = ?, summary_json = ? WHERE id = ? AND status = ?`
	res, err := r.db.ExecContext(ctx, stmt, LotStatusCompleted, summary.CompletedAt.UTC(), string(payload), lotID, LotStatusProcessing)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Summary converts the raw summary payload to a typed structure when available.
func (l Lot) Summary() (*LotSummary, error) {
	if len(l.SummaryJSON) == 0 {
		return nil, nil
	}
	var summary LotSummary
	if err := json.Unmarshal(l.SummaryJSON, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

// ListProductData returns lot records transformed to product-centric payloads.
func (r *Repository) ListProductData(ctx context.Context) ([]ProductData, error) {
	lots, err := r.ListLots(ctx)
	if err != nil {
		return nil, err
	}

	products := make([]ProductData, 0, len(lots))
	now := time.Now().UTC()
	for _, lot := range lots {
		product, err := lotToProductData(lot, now)
		if err != nil {
			return nil, err
		}
		products = append(products, product)
	}

	return products, nil
}

// BackfillCandidate represents a lot row needing computed fields.
type BackfillCandidate struct {
	ID          int64
	LotNumber   string
	MachineName string
	StartedAt   time.Time
	CompletedAt sql.NullTime
}

// ListCompletedLotsMissingData returns completed lots where averages or operation_hour are missing.
func (r *Repository) ListCompletedLotsMissingData(ctx context.Context) ([]BackfillCandidate, error) {
	const query = `SELECT id, lot_number, machine_name, started_at, completed_at FROM lots WHERE status = ? AND (averages_json IS NULL OR operation_hour IS NULL)`
	rows, err := r.db.QueryContext(ctx, query, LotStatusCompleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []BackfillCandidate
	for rows.Next() {
		var b BackfillCandidate
		if err := rows.Scan(&b.ID, &b.LotNumber, &b.MachineName, &b.StartedAt, &b.CompletedAt); err != nil {
			return nil, err
		}
		list = append(list, b)
	}
	return list, rows.Err()
}

// UpdateLotComputedFields sets averages_json and/or operation_hour for a lot.
// If a parameter is nil the corresponding column is left unchanged.
func (r *Repository) UpdateLotComputedFields(ctx context.Context, id int64, opHour *string, averagesJSON *string) error {
	const stmt = `UPDATE lots SET averages_json = COALESCE(?, averages_json), operation_hour = COALESCE(?, operation_hour), updated_at = NOW() WHERE id = ?`
	var avgParam interface{}
	if averagesJSON == nil {
		avgParam = nil
	} else {
		avgParam = *averagesJSON
	}
	var opParam interface{}
	if opHour == nil {
		opParam = nil
	} else {
		opParam = *opHour
	}
	_, err := r.db.ExecContext(ctx, stmt, avgParam, opParam, id)
	return err
}

func lotToProductData(lot Lot, fallbackNow time.Time) (ProductData, error) {
	summary, err := lot.Summary()
	if err != nil {
		return ProductData{}, fmt.Errorf("parse summary for lot %s: %w", lot.LotNumber, err)
	}

	averages, err := decodeLotAverages(lot.Averages)
	if err != nil {
		return ProductData{}, fmt.Errorf("parse averages for lot %s: %w", lot.LotNumber, err)
	}
	if len(averages) == 0 && summary != nil {
		for _, sensor := range summary.Sensors {
			name := strings.TrimSpace(sensor.SensorName)
			if name == "" {
				continue
			}
			averages[strings.ToLower(name)] = sensor.LatestValue
		}
	}

	operationHour := resolveOperationHours(lot, fallbackNow)

	goodProduct := 0
	if lot.GoodProduct != nil {
		goodProduct = *lot.GoodProduct
	} else if summary != nil {
		goodProduct = summary.GoodProduct
	}

	defectProduct := 0
	if lot.DefectProduct != nil {
		defectProduct = *lot.DefectProduct
	} else if summary != nil {
		defectProduct = summary.DefectProduct
	}

	conclusion := ""
	if lot.Conclusion != nil && strings.TrimSpace(*lot.Conclusion) != "" {
		conclusion = strings.TrimSpace(*lot.Conclusion)
	} else if summary != nil {
		conclusion = summary.Conclusion
	}

	activeMachineID := lot.MachineName
	if lot.ActiveMachineID != nil && strings.TrimSpace(*lot.ActiveMachineID) != "" {
		activeMachineID = strings.TrimSpace(*lot.ActiveMachineID)
	}

	updatedAt := lot.UpdatedAt
	if updatedAt.IsZero() {
		switch {
		case lot.CompletedAt.Valid:
			updatedAt = lot.CompletedAt.Time
		case summary != nil && !summary.CompletedAt.IsZero():
			updatedAt = summary.CompletedAt
		default:
			updatedAt = fallbackNow
		}
	}

	return ProductData{
		Lot:             lot.LotNumber,
		Status:          lot.Status,
		ActiveMachineID: activeMachineID,
		Averages:        averages,
		OperationHour:   operationHour,
		GoodProduct:     goodProduct,
		DefectProduct:   defectProduct,
		Conclusion:      conclusion,
		IsConclusion:    lot.IsConclusion,
		UpdatedAt:       updatedAt,
	}, nil
}

func decodeLotAverages(raw json.RawMessage) (map[string]float64, error) {
	if len(raw) == 0 {
		return map[string]float64{}, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}

	averages := make(map[string]float64, len(payload))
	for key, value := range payload {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" {
			continue
		}

		switch typed := value.(type) {
		case json.Number:
			if f, err := typed.Float64(); err == nil {
				averages[normalizedKey] = f
			}
		case float64:
			averages[normalizedKey] = typed
		case string:
			sanitized := strings.ReplaceAll(strings.TrimSpace(typed), ",", ".")
			if sanitized == "" {
				continue
			}
			if f, err := strconv.ParseFloat(sanitized, 64); err == nil {
				averages[normalizedKey] = f
			}
		default:
			// unsupported type, ignore
		}
	}

	return averages, nil
}

func resolveOperationHours(lot Lot, defaultEnd time.Time) float64 {
	if lot.OperationHour != nil {
		value := strings.TrimSpace(*lot.OperationHour)
		if value == "" {
			return computeOperationHours(lot, defaultEnd)
		}
		sanitized := strings.ReplaceAll(value, ",", ".")
		if hours, err := strconv.ParseFloat(sanitized, 64); err == nil {
			return hours
		}
	}
	return computeOperationHours(lot, defaultEnd)
}

func computeOperationHours(lot Lot, defaultEnd time.Time) float64 {
	end := defaultEnd
	if lot.CompletedAt.Valid {
		end = lot.CompletedAt.Time
	}
	if end.Before(lot.StartedAt) {
		return 0
	}
	duration := end.Sub(lot.StartedAt).Hours()
	if duration < 0 {
		duration = 0
	}
	return math.Round(duration*10) / 10
}

func isDuplicateEntry(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "duplicate")
}

func mapLotError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLotNotFound
	}
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanLot(scanner rowScanner) (Lot, error) {
	var (
		lot           Lot
		completedAt   sql.NullTime
		summary       sql.NullString
		activeID      sql.NullString
		averages      sql.NullString
		opHour        sql.NullString
		goodProduct   sql.NullInt64
		defectProduct sql.NullInt64
		conclusion    sql.NullString
		isConclusion  sql.NullBool
	)
	if err := scanner.Scan(
		&lot.ID,
		&lot.LotNumber,
		&lot.MachineName,
		&lot.Status,
		&lot.StartedAt,
		&completedAt,
		&lot.UpdatedAt,
		&summary,
		&activeID,
		&averages,
		&opHour,
		&goodProduct,
		&defectProduct,
		&conclusion,
		&isConclusion,
	); err != nil {
		return Lot{}, err
	}
	lot.CompletedAt = completedAt
	if summary.Valid {
		lot.SummaryJSON = json.RawMessage(summary.String)
	}
	if activeID.Valid {
		value := strings.TrimSpace(activeID.String)
		if value != "" {
			lot.ActiveMachineID = &value
		}
	}
	if averages.Valid {
		lot.Averages = json.RawMessage(averages.String)
	}
	if opHour.Valid {
		value := strings.TrimSpace(opHour.String)
		if value != "" {
			lot.OperationHour = &value
		}
	}
	if goodProduct.Valid {
		val := int(goodProduct.Int64)
		lot.GoodProduct = &val
	}
	if defectProduct.Valid {
		val := int(defectProduct.Int64)
		lot.DefectProduct = &val
	}
	if conclusion.Valid {
		value := strings.TrimSpace(conclusion.String)
		if value != "" {
			lot.Conclusion = &value
		}
	}
	if isConclusion.Valid {
		lot.IsConclusion = isConclusion.Bool
	}
	return lot, nil
}

func toNullString(value *string) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

func toNullRawMessage(value json.RawMessage) sql.NullString {
	if len(value) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: string(value), Valid: true}
}

func toNullInt(value *int) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*value), Valid: true}
}

func toNullBool(value *bool) sql.NullBool {
	if value == nil {
		return sql.NullBool{}
	}
	return sql.NullBool{Bool: *value, Valid: true}
}

// DeleteLotByNumber removes a lot (and its product data) by lot number.
// Returns ErrLotNumberRequired when lotNumber is empty and ErrLotNotFound
// when no matching row exists.
func (r *Repository) DeleteLotByNumber(ctx context.Context, lotNumber string) error {
	lotNumber = strings.TrimSpace(lotNumber)
	if lotNumber == "" {
		return ErrLotNumberRequired
	}
	const stmt = `DELETE FROM lots WHERE lot_number = ?`
	res, err := r.db.ExecContext(ctx, stmt, lotNumber)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrLotNotFound
	}
	return nil
}
