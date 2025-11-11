package server

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Resanso/minerva-ericsson/apps/api/internal/influxdb"
	"github.com/Resanso/minerva-ericsson/apps/api/internal/simulation"
)

// Dependencies groups objects the HTTP layer needs.
type Dependencies struct {
	Simulator *simulation.Simulator
	Influx    *influxdb.Client
}

// NewRouter configures all HTTP routes.
func NewRouter(deps Dependencies) *gin.Engine {
	r := gin.Default()

	r.GET("/api/hello", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Hello from Go Gin Backend!"})
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

	return r
}
