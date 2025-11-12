package llm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/generative-ai-go/genai"
	"google.golang.org/api/option"
)

const (
	defaultModel       = "gemini-1.5-flash"
	defaultTemperature = 0.2
)

var (
	// ErrMissingAPIKey is returned when no API key is provided via environment variables.
	ErrMissingAPIKey = errors.New("missing GEMINI_API_KEY or LLM_API_KEY")
)

// Config captures the parameters required to instantiate the LLM client.
type Config struct {
	APIKey      string
	Model       string
	Temperature float32
	TopP        *float32
	TopK        *int32
}

// FromEnv builds a Config from well-known environment variables. GEMINI_API_KEY or LLM_API_KEY is required.
func FromEnv() (Config, error) {
	apiKey := strings.TrimSpace(os.Getenv("GEMINI_API_KEY"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(os.Getenv("LLM_API_KEY"))
	}
	if apiKey == "" {
		return Config{}, ErrMissingAPIKey
	}

	cfg := Config{
		APIKey:      apiKey,
		Model:       strings.TrimSpace(os.Getenv("GEMINI_MODEL")),
		Temperature: defaultTemperature,
	}
	if cfg.Model == "" {
		cfg.Model = defaultModel
	}

	if tempStr := strings.TrimSpace(os.Getenv("GEMINI_TEMPERATURE")); tempStr != "" {
		if val, err := strconv.ParseFloat(tempStr, 32); err == nil {
			cfg.Temperature = float32(val)
		}
	}
	if topPStr := strings.TrimSpace(os.Getenv("GEMINI_TOP_P")); topPStr != "" {
		if val, err := strconv.ParseFloat(topPStr, 32); err == nil {
			f32 := float32(val)
			cfg.TopP = &f32
		}
	}
	if topKStr := strings.TrimSpace(os.Getenv("GEMINI_TOP_K")); topKStr != "" {
		if val, err := strconv.ParseInt(topKStr, 10, 32); err == nil {
			i32 := int32(val)
			cfg.TopK = &i32
		}
	}

	return cfg, nil
}

// Client wraps the Gemini SDK to provide higher-level helpers for the chatbot workflow.
type Client struct {
	client      *genai.Client
	modelName   string
	temperature float32
	topP        *float32
	topK        *int32
}

// New instantiates a Client using the provided configuration.
func New(ctx context.Context, cfg Config) (*Client, error) {
	genClient, err := genai.NewClient(ctx, option.WithAPIKey(cfg.APIKey))
	if err != nil {
		return nil, fmt.Errorf("create generative ai client: %w", err)
	}

	return &Client{
		client:      genClient,
		modelName:   cfg.Model,
		temperature: cfg.Temperature,
		topP:        cfg.TopP,
		topK:        cfg.TopK,
	}, nil
}

// Close releases the underlying SDK resources.
func (c *Client) Close() error {
	return c.client.Close()
}

// GenerateText executes a single request-response interaction with the configured model.
func (c *Client) GenerateText(ctx context.Context, systemPrompt string, userParts ...string) (string, error) {
	if len(userParts) == 0 {
		return "", fmt.Errorf("user prompt is required")
	}

	model := c.client.GenerativeModel(c.modelName)
	c.applyGenerationConfig(model)

	if systemPrompt != "" {
		model.SystemInstruction = &genai.Content{Role: "system", Parts: []genai.Part{genai.Text(systemPrompt)}}
	}

	parts := make([]genai.Part, 0, len(userParts))
	for _, part := range userParts {
		text := strings.TrimSpace(part)
		if text == "" {
			continue
		}
		parts = append(parts, genai.Text(text))
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("user prompt is empty")
	}

	resp, err := model.GenerateContent(ctx, parts...)
	if err != nil {
		return "", fmt.Errorf("generate content: %w", err)
	}

	return extractText(resp)
}

func (c *Client) applyGenerationConfig(model *genai.GenerativeModel) {
	model.GenerationConfig.SetTemperature(c.temperature)
	if c.topP != nil {
		model.GenerationConfig.SetTopP(*c.topP)
	}
	if c.topK != nil {
		model.GenerationConfig.SetTopK(*c.topK)
	}
}

func extractText(resp *genai.GenerateContentResponse) (string, error) {
	if resp == nil {
		return "", fmt.Errorf("empty LLM response")
	}
	for _, cand := range resp.Candidates {
		if cand == nil || cand.Content == nil {
			continue
		}
		var sb strings.Builder
		for _, part := range cand.Content.Parts {
			switch v := part.(type) {
			case genai.Text:
				sb.WriteString(string(v))
			case *genai.Text:
				sb.WriteString(string(*v))
			}
		}
		if sb.Len() > 0 {
			return sb.String(), nil
		}
	}
	return "", fmt.Errorf("no text candidates in LLM response")
}
