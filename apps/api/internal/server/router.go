package server

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/llm"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/metadata"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/simulation"
)

// Dependencies groups objects the HTTP layer needs.
type Dependencies struct {
	Simulator *simulation.Simulator
	Influx    *influxdb.Client
	Metadata  *metadata.Repository
	LLM       *llm.Client
}

// NewRouter configures all HTTP routes.
func NewRouter(deps Dependencies) *gin.Engine {
	r := gin.Default()

	corsConfig := cors.Config{
		AllowOrigins: []string{
			"http://localhost:3000",
			"https://localhost:3000",
		},
		AllowMethods:        []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:        []string{"Origin", "Content-Type", "Accept"},
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
		})
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrLotNumberRequired):
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			case errors.Is(err, metadata.ErrMachineNameRequired):
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			default:
				log.Printf("upsert product failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to upsert product"})
			}
			return
		}

		c.JSON(http.StatusOK, product)
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
			case errors.Is(err, metadata.ErrMachineNameRequired):
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

	return r
}
