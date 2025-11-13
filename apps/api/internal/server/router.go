package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	influx "github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/llm"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/metadata"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/simulation"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

// Dependencies groups external services required by the HTTP handlers.
type Dependencies struct {
	Simulator *simulation.Simulator
	Influx    *influx.Client
	Metadata  *metadata.Repository
	LLM       *llm.Client
}

// NewRouter creates a gin.Engine configured with routes and middleware.
func NewRouter(deps Dependencies) *gin.Engine {
	r := gin.Default()

	corsConfig := cors.Config{
		AllowOrigins:     []string{"http://localhost:3000", "https://localhost:3000"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
		AllowPrivateNetwork: true,
	}
	corsConfig.AllowWildcard = true
	corsConfig.AllowHeaders = append(corsConfig.AllowHeaders, "Authorization")
	r.Use(cors.New(corsConfig))

	r.GET("/api/hello", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Hello from Go Gin Backend!"})
	})

	r.POST("/api/chatbot/query", func(c *gin.Context) {
		HandleChatQuery(c, deps)
	})

	r.GET("/api/influx/ping", func(c *gin.Context) {
		if deps.Influx == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "missing client"})
			return
		}
		if err := deps.Influx.Ping(c.Request.Context()); err != nil {
			log.Printf("influx ping failed: %v", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/api/influx/stream", func(c *gin.Context) {
		if deps.Influx == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "influx client unavailable"})
			return
		}

		measurement := c.DefaultQuery("measurement", "sensor_data")
		lookback := time.Minute
		if raw := c.Query("lookback"); raw != "" {
			if dur, err := time.ParseDuration(raw); err == nil && dur > 0 {
				lookback = dur
			}
		}

		pollInterval := 2 * time.Second
		if raw := c.Query("interval"); raw != "" {
			if dur, err := time.ParseDuration(raw); err == nil && dur > 0 {
				pollInterval = dur
			}
		}

		filters := map[string]string{}
		if machine := c.Query("machine"); machine != "" {
			filters["machine_name"] = machine
		}
		if sensor := c.Query("sensor"); sensor != "" {
			filters["sensor_name"] = sensor
		}
		if len(filters) == 0 {
			filters = nil
		}

		ctx := c.Request.Context()
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("Transfer-Encoding", "chunked")

		initialStart := time.Now().Add(-lookback)
		var lastSent time.Time

		pollTicker := time.NewTicker(pollInterval)
		keepAliveTicker := time.NewTicker(30 * time.Second)
		defer pollTicker.Stop()
		defer keepAliveTicker.Stop()

		type readingPayload struct {
			Time        string  `json:"time"`
			MachineName string  `json:"machineName"`
			SensorName  string  `json:"sensorName"`
			Value       float64 `json:"value"`
		}

		c.Stream(func(w io.Writer) bool {
			select {
			case <-ctx.Done():
				return false
			case <-pollTicker.C:
				start := initialStart
				if !lastSent.IsZero() {
					start = lastSent.Add(time.Nanosecond)
				}

				readings, err := deps.Influx.SensorReadingsSince(ctx, measurement, start, filters, 0)
				if err != nil {
					log.Printf("stream sensor readings failed: %v", err)
					c.Render(-1, sse.Event{
						Event: "error",
						Data:  "failed to query sensor readings",
					})
					return true
				}

				for _, reading := range readings {
					if reading.Time.IsZero() {
						continue
					}
					c.Render(-1, sse.Event{
						Event: "reading",
						Data: readingPayload{
							Time:        reading.Time.UTC().Format(time.RFC3339Nano),
							MachineName: reading.MachineName,
							SensorName:  reading.SensorName,
							Value:       reading.Value,
						},
					})
					if reading.Time.After(lastSent) {
						lastSent = reading.Time
					}
				}
				return true
			case <-keepAliveTicker.C:
				c.Writer.Write([]byte(": keep-alive\n\n"))
				c.Writer.Flush()
				return true
			}
		})
	})

	r.GET("/api/simulation/status", func(c *gin.Context) {
		if deps.Simulator == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"running": false})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"running":  true,
			"interval": deps.Simulator.Interval().String(),
			"sensors":  deps.Simulator.Snapshot(),
		})
	})

	r.GET("/api/mysql/ping", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "missing repository"})
			return
		}
		if err := deps.Metadata.Ping(c.Request.Context()); err != nil {
			log.Printf("mysql ping failed: %v", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.GET("/api/lots", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"lots": []metadata.Lot{}})
			return
		}
		lots, err := deps.Metadata.ListLots(c.Request.Context())
		if err != nil {
			log.Printf("list lots failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list lots"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"lots": lots})
	})

	r.GET("/api/products", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"products": []metadata.ProductData{}})
			return
		}
		products, err := deps.Metadata.ListProductData(c.Request.Context())
		if err != nil {
			log.Printf("list products failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list products"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"products": products})
	})

	// (DELETE /api/products/:lotNumber) -- handler preserved later in file; avoid duplicate registration.

	r.POST("/api/products", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata repository unavailable"})
			return
		}
		var req struct {
			LotNumber       string          `json:"lotNumber"`
			MachineName     string          `json:"machineName"`
			ActiveMachineID *string         `json:"activeMachineId"`
			Averages        json.RawMessage `json:"averages"`
			OperationHour   *string         `json:"operationHour"`
			GoodProduct     *int            `json:"goodProduct"`
			DefectProduct   *int            `json:"defectProduct"`
			Conclusion      *string         `json:"conclusion"`
			IsConclusion    *bool           `json:"isConclusion"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			log.Printf("invalid product payload: %v", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
			return
		}

		product, err := deps.Metadata.UpsertLotProduct(c.Request.Context(), metadata.ProductInput{
			LotNumber:       req.LotNumber,
			MachineName:     req.MachineName,
			ActiveMachineID: req.ActiveMachineID,
			Averages:        req.Averages,
			OperationHour:   req.OperationHour,
			GoodProduct:     req.GoodProduct,
			DefectProduct:   req.DefectProduct,
				Conclusion:      req.Conclusion,
				IsConclusion:    req.IsConclusion,
		})
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrLotNumberRequired):
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			default:
				log.Printf("upsert product failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to upsert product"})
			}
			return
		}

		c.JSON(http.StatusOK, product)
	})

	// DELETE a product (lot) by its lot number
	r.DELETE("/api/products/:lotNumber", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata repository unavailable"})
			return
		}

		lot := c.Param("lotNumber")
		if strings.TrimSpace(lot) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "lot number is required"})
			return
		}

		if err := deps.Metadata.DeleteLotByNumber(c.Request.Context(), lot); err != nil {
			switch {
			case errors.Is(err, metadata.ErrLotNotFound):
				c.JSON(http.StatusNotFound, gin.H{"error": "lot not found"})
			default:
				log.Printf("delete lot failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete lot"})
			}
			return
		}

		c.Status(http.StatusNoContent)
	})

	r.POST("/api/lots", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata repository unavailable"})
			return
		}
		var req struct {
			LotNumber   string `json:"lotNumber"`
			MachineName string `json:"machineName"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
			return
		}
		lot, err := deps.Metadata.CreateLot(c.Request.Context(), metadata.CreateLotInput{
			LotNumber:   req.LotNumber,
			MachineName: req.MachineName,
		})
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrLotNumberRequired):
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			case errors.Is(err, metadata.ErrLotExists):
				c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			default:
				log.Printf("create lot failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create lot"})
			}
			return
		}
		c.JSON(http.StatusCreated, lot)
	})

	r.GET("/api/machines", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"machines": []metadata.Machine{}})
			return
		}
		machines, err := deps.Metadata.ListMachines(c.Request.Context())
		if err != nil {
			log.Printf("list machines failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list machines"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"machines": machines})
	})

	r.POST("/api/machines", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata repository unavailable"})
			return
		}
		var req struct {
			MachineName string `json:"machineName"`
			Location    string `json:"location"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
			return
		}
		created, err := deps.Metadata.CreateMachine(c.Request.Context(), metadata.CreateMachineInput{
			MachineName: req.MachineName,
			Location:    req.Location,
		})
		if err != nil {
			if errors.Is(err, metadata.ErrMachineNameRequired) {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			log.Printf("create machine failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create machine"})
			return
		}
		c.JSON(http.StatusCreated, created)
	})

	// Backfill computed product fields (operation_hour, averages_json) for completed lots
	r.POST("/api/lots/backfill", func(c *gin.Context) {
		if deps.Metadata == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metadata repository unavailable"})
			return
		}
		if deps.Influx == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "influx client unavailable"})
			return
		}

		ctx := c.Request.Context()
		candidates, err := deps.Metadata.ListCompletedLotsMissingData(ctx)
		if err != nil {
			log.Printf("list backfill candidates failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list backfill candidates"})
			return
		}

		measurement := c.DefaultQuery("measurement", "sensor_data")
		updated := []string{}
		for _, cand := range candidates {
			if !cand.CompletedAt.Valid {
				continue
			}
			start := cand.StartedAt.UTC()
			end := cand.CompletedAt.Time.UTC()
			if end.Before(start) {
				// skip invalid ranges
				continue
			}

			// compute operation hours (one decimal place)
			hours := end.Sub(start).Hours()
			hours = math.Round(hours*10) / 10
			opStr := fmt.Sprintf("%.1f", hours)

			// build flux query to compute mean per sensor_name
			flux := fmt.Sprintf(`from(bucket: %q)
|> range(start: time(v: %q), stop: time(v: %q))
|> filter(fn: (r) => r["_measurement"] == %q)
|> filter(fn: (r) => r["_field"] == "value")
|> filter(fn: (r) => r[%q] == %q)
|> group(columns: ["sensor_name"])
|> mean()
|> keep(columns: ["sensor_name", "_value"])`, deps.Influx.Config().Bucket, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), measurement, "machine_name", cand.MachineName)

			result, qerr := deps.Influx.QueryAPI().Query(ctx, flux)
			if qerr != nil {
				log.Printf("influx query failed for lot %s: %v", cand.LotNumber, qerr)
				continue
			}

			averages := map[string]float64{}
			for result.Next() {
				rec := result.Record()
				// sensor name
				sName := fmt.Sprint(rec.ValueByKey("sensor_name"))
				// value is in _value
				valAny := rec.Value()
				var v float64
				switch t := valAny.(type) {
				case float64:
					v = t
				case int64:
					v = float64(t)
				case uint64:
					v = float64(t)
				default:
					continue
				}
				if sName != "" {
					averages[sName] = v
				}
			}
			if err := result.Err(); err != nil {
				log.Printf("iterate influx result failed for lot %s: %v", cand.LotNumber, err)
			}

			// marshal averages to JSON
			avgJSON, _ := json.Marshal(averages)
			avgStr := string(avgJSON)

			if err := deps.Metadata.UpdateLotComputedFields(ctx, cand.ID, &opStr, &avgStr); err != nil {
				log.Printf("update lot computed failed for lot %s: %v", cand.LotNumber, err)
				continue
			}
			updated = append(updated, cand.LotNumber)
		}

		c.JSON(http.StatusOK, gin.H{"updated": updated})
	})

	return r
}
