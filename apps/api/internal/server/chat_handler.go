package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Resanso/minerva-ericsson/apps/api/internal/simulation"
)

const (
	fluxSystemPromptHeader = "Anda adalah asisten AI yang ahli dalam menyusun query Flux untuk InfluxDB. Gunakan informasi skema berikut untuk menerjemahkan pertanyaan pengguna ke query Flux yang valid. Kembalikan HANYA kode Flux tanpa penjelasan atau pembungkus markdown."
	analysisSystemPrompt   = "Anda adalah analis data manufaktur. Gunakan data CSV yang diberikan untuk menjawab pertanyaan secara ringkas dan akurat. Jika data kosong, jelaskan bahwa data tidak tersedia."
)

type chatQueryRequest struct {
	Question string `json:"question"`
}

// HandleChatQuery orchestrates the text-to-Flux-to-answer workflow described in the LLM integration design.
func HandleChatQuery(c *gin.Context, deps Dependencies) {
	if deps.LLM == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "LLM client not configured"})
		return
	}
	if deps.Influx == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "InfluxDB client not configured"})
		return
	}

	var req chatQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}

	question := strings.TrimSpace(req.Question)
	if question == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "question is required"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	defer cancel()

	cfg := deps.Influx.Config()
	fluxSystemPrompt := buildFluxSystemPrompt(cfg.Bucket, simulation.MeasurementName())

	fluxQueryRaw, err := deps.LLM.GenerateText(ctx, fluxSystemPrompt, question)
	if err != nil {
		log.Printf("llm flux generation failed: %v", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to generate Flux query"})
		return
	}

	fluxQuery := normalizeFluxQuery(fluxQueryRaw)
	if fluxQuery == "" {
		log.Printf("llm returned empty flux query; raw=%q", fluxQueryRaw)
		c.JSON(http.StatusBadGateway, gin.H{"error": "LLM produced an empty Flux query"})
		return
	}

	rawResult, err := deps.Influx.QueryAPI().QueryRaw(ctx, fluxQuery, nil)
	if err != nil {
		log.Printf("flux query execution failed: %v; query=%s", err, fluxQuery)
		c.JSON(http.StatusBadRequest, gin.H{"error": "flux query execution failed", "fluxQuery": fluxQuery})
		return
	}

	analysisPrompt := buildAnalysisPrompt(question, rawResult)

	answer, err := deps.LLM.GenerateText(ctx, analysisSystemPrompt, analysisPrompt)
	if err != nil {
		log.Printf("llm analysis failed: %v", err)
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to interpret query result", "fluxQuery": fluxQuery, "data": rawResult})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"answer":    strings.TrimSpace(answer),
		"fluxQuery": fluxQuery,
		"data":      rawResult,
	})
}

func buildFluxSystemPrompt(bucket, measurement string) string {
	var sb strings.Builder
	sb.WriteString(fluxSystemPromptHeader)
	sb.WriteString("\n\n")
	sb.WriteString(fmt.Sprintf("Skema:\n- Bucket: %s\n- Measurement: %s\n- Field numerik: \"value\"\n- Tag: \"machine_name\", \"sensor_name\"\n", bucket, measurement))
	ss := describeAvailableSensors()
	if ss != "" {
		sb.WriteString("\nSensor yang tersedia:\n")
		sb.WriteString(ss)
		sb.WriteString("\n")
	}
	sb.WriteString("\nPastikan query menyertakan filter _field == \"value\" dan gunakan rentang waktu yang relevan.\n")
	return sb.String()
}

func describeAvailableSensors() string {
	sensors := simulation.DefaultSensors()
	if len(sensors) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(sensors))
	entries := make([]string, 0, len(sensors))
	for _, sensor := range sensors {
		if sensor == nil {
			continue
		}
		key := strings.ToLower(sensor.MachineName + "|" + sensor.SensorName)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		entries = append(entries, fmt.Sprintf("- machine_name=\"%s\", sensor_name=\"%s\"", sensor.MachineName, sensor.SensorName))
	}
	if len(entries) == 0 {
		return ""
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}

func normalizeFluxQuery(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	trimmed = strings.TrimPrefix(trimmed, "```flux")
	trimmed = strings.TrimPrefix(trimmed, "```")
	if idx := strings.LastIndex(trimmed, "```"); idx >= 0 {
		trimmed = trimmed[:idx]
	}
	return strings.TrimSpace(trimmed)
}

func buildAnalysisPrompt(question, rawData string) string {
	cleanData := strings.TrimSpace(rawData)
	if cleanData == "" {
		cleanData = "(data kosong)"
	}
	return fmt.Sprintf("Data hasil query (format CSV):\n%s\n\nPertanyaan pengguna: %s\nBerikan jawaban yang jelas dan ringkas berdasarkan data di atas.", cleanData, question)
}
