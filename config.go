package main

import (
	"strings"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

const (
	pluginName    = "cpa-plugin-codex-overload-retry"
	pluginVersion = "0.1.0"
	// ABI schema version of the host this plugin targets (CLIProxyAPI 8.0.52 = schema 10).
	pluginSchemaVersion = 10
)

type pluginConfig struct {
	Enabled               bool     `yaml:"enabled"`
	SourceFormats         []string `yaml:"source_formats"`
	RequireCodexProvider  bool     `yaml:"require_codex_provider"`
	MaxAttempts           int      `yaml:"max_attempts"`
	TotalBudgetSeconds    float64  `yaml:"total_budget_seconds"`
	CommitDeadlineSeconds float64  `yaml:"commit_deadline_seconds"`
	BackoffBaseSeconds    float64  `yaml:"backoff_base_seconds"`
	BackoffFactor         float64  `yaml:"backoff_factor"`
	BackoffMaxSeconds     float64  `yaml:"backoff_max_seconds"`
	MaxRetryWaitSeconds   float64  `yaml:"max_retry_wait_seconds"`
	PreludeLimitBytes     int      `yaml:"prelude_limit_bytes"`
	RetryHTTPStatus       []int    `yaml:"retry_http_status"`
	LogRetries            bool     `yaml:"log_retries"`
	LogRoutes             bool     `yaml:"log_routes"`

	sourceFormats map[string]bool `yaml:"-"`
	retryStatuses map[int]bool   `yaml:"-"`
}

func defaultConfig() pluginConfig {
	return pluginConfig{
		Enabled:               true,
		SourceFormats:         []string{"openai-response", "openai"},
		RequireCodexProvider:  true,
		MaxAttempts:           30,
		TotalBudgetSeconds:    900,
		CommitDeadlineSeconds: 120,
		BackoffBaseSeconds:    1,
		BackoffFactor:         1.6,
		BackoffMaxSeconds:     8,
		MaxRetryWaitSeconds:   300,
		PreludeLimitBytes:     256 * 1024,
		RetryHTTPStatus:       []int{429, 500, 502, 503, 504},
		LogRetries:            true,
		LogRoutes:             true,
	}
}

func decodeConfig(raw []byte) (pluginConfig, error) {
	cfg := defaultConfig()
	if len(raw) > 0 {
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return pluginConfig{}, err
		}
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.TotalBudgetSeconds <= 0 {
		cfg.TotalBudgetSeconds = 300
	}
	if cfg.CommitDeadlineSeconds <= 0 {
		cfg.CommitDeadlineSeconds = 120
	}
	if cfg.BackoffBaseSeconds <= 0 {
		cfg.BackoffBaseSeconds = 1
	}
	if cfg.BackoffFactor <= 1 {
		cfg.BackoffFactor = 1.6
	}
	if cfg.BackoffMaxSeconds <= 0 {
		cfg.BackoffMaxSeconds = 8
	}
	if cfg.MaxRetryWaitSeconds <= 0 {
		cfg.MaxRetryWaitSeconds = 300
	}
	if cfg.PreludeLimitBytes <= 0 {
		cfg.PreludeLimitBytes = 256 * 1024
	}
	cfg.sourceFormats = make(map[string]bool, len(cfg.SourceFormats))
	for _, f := range cfg.SourceFormats {
		f = strings.ToLower(strings.TrimSpace(f))
		if f != "" {
			cfg.sourceFormats[f] = true
		}
	}
	cfg.retryStatuses = make(map[int]bool, len(cfg.RetryHTTPStatus))
	for _, s := range cfg.RetryHTTPStatus {
		cfg.retryStatuses[s] = true
	}
	return cfg, nil
}

var currentConfig atomic.Value

func storeConfig(cfg pluginConfig) {
	currentConfig.Store(cfg)
}

func loadConfig() pluginConfig {
	if v, ok := currentConfig.Load().(pluginConfig); ok {
		return v
	}
	return defaultConfig()
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if err := jsonUnmarshal(raw, &req); err != nil {
			return err
		}
	}
	cfg, err := decodeConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	storeConfig(cfg)
	return nil
}

// pluginRegistration builds the registration from cfg so executor input/output
// formats always cover the configured source formats (reconfigure re-reads it).
func pluginRegistration(cfg pluginConfig) registration {
	formats := make([]string, 0, len(cfg.SourceFormats))
	for _, f := range cfg.SourceFormats {
		f = strings.ToLower(strings.TrimSpace(f))
		if f != "" {
			formats = append(formats, f)
		}
	}
	if len(formats) == 0 {
		formats = []string{"openai-response", "openai"}
	}
	return registration{
		SchemaVersion: pluginSchemaVersion,
		Metadata: metadata{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           "D4wnnn",
			GitHubRepository: "https://github.com/D4wnnn/cpa-plugin-codex-overload-retry",
			ConfigFields: []configField{
				{Name: "enabled", Type: "boolean", Default: true, Description: "Master switch; when false all requests fall through to the built-in path."},
				{Name: "source_formats", Type: "array", Default: []string{"openai-response", "openai"}, Description: "Client protocols whose codex-bound requests are wrapped with overload retry."},
				{Name: "require_codex_provider", Type: "boolean", Default: true, Description: "Only handle requests whose model is registered under codex credentials (host.auth.models lookup)."},
				{Name: "max_attempts", Type: "integer", Default: 30, Description: "Maximum upstream attempts per request."},
				{Name: "total_budget_seconds", Type: "number", Default: 900, Description: "Total retry budget in seconds per request."},
				{Name: "commit_deadline_seconds", Type: "number", Default: 120, Description: "Max seconds to hold the stream head while deciding to retry."},
				{Name: "backoff_base_seconds", Type: "number", Default: 1, Description: "First retry delay in seconds."},
				{Name: "backoff_factor", Type: "number", Default: 1.6, Description: "Exponential backoff multiplier."},
				{Name: "backoff_max_seconds", Type: "number", Default: 8, Description: "Backoff delay cap in seconds."},
				{Name: "max_retry_wait_seconds", Type: "number", Default: 300, Description: "Cap for a single retry wait when honoring upstream retry-after guidance."},
				{Name: "prelude_limit_bytes", Type: "integer", Default: 262144, Description: "Buffered stream head limit; beyond it the stream is committed."},
				{Name: "retry_http_status", Type: "array", Default: []int{429, 500, 502, 503, 504}, Description: "Upstream HTTP status codes that trigger a retry when not a denied error."},
				{Name: "log_retries", Type: "boolean", Default: true, Description: "Log each absorbed retry through host.log."},
				{Name: "log_routes", Type: "boolean", Default: true, Description: "Log each request wrapped by the plugin through host.log."},
			},
		},
		Capabilities: capabilities{
			ModelRouter:           true,
			Executor:              true,
			ExecutorModelScope:    "both",
			ExecutorInputFormats:  formats,
			ExecutorOutputFormats: formats,
			ManagementAPI:         true,
		},
	}
}
