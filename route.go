package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// routeModel decides whether this request should be wrapped with overload
// retry. Requests are handled when:
//   - the plugin is enabled,
//   - the client protocol is in source_formats (default: openai-response, openai),
//   - the requested model is registered under codex credentials (unless
//     require_codex_provider is false).
func routeModel(raw []byte) ([]byte, error) {
	var req modelRouteRequest
	if err := jsonUnmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := loadConfig()
	if !cfg.Enabled {
		return okEnvelope(modelRouteResponse{Handled: false})
	}
	sourceFormat := strings.ToLower(strings.TrimSpace(req.SourceFormat))
	if !cfg.sourceFormats[sourceFormat] {
		logHost("debug", "cpa-plugin-codex-overload-retry: route declined (source format)", map[string]any{
			"sourceFormat": sourceFormat,
			"model":        req.RequestedModel,
		})
		return okEnvelope(modelRouteResponse{Handled: false})
	}
	model := strings.TrimSpace(req.RequestedModel)
	if model == "" {
		return okEnvelope(modelRouteResponse{Handled: false})
	}
	if cfg.RequireCodexProvider && !codexProviderServes(sourceFormat, model) {
		logHost("debug", "cpa-plugin-codex-overload-retry: route declined (model not under codex credentials)", map[string]any{
			"sourceFormat": sourceFormat,
			"model":        model,
		})
		return okEnvelope(modelRouteResponse{Handled: false})
	}
	if cfg.LogRoutes {
		stream := "stream"
		if !req.Stream {
			stream = "sync"
		}
		logHost("info", fmt.Sprintf("cpa-plugin-codex-overload-retry: wrapping %s request for %s", stream, model), map[string]any{
			"model":        model,
			"sourceFormat": sourceFormat,
			"stream":       req.Stream,
		})
	}
	stats.recordWrapped(model, req.Stream)
	return okEnvelope(modelRouteResponse{
		Handled:    true,
		TargetKind: "self",
		Reason:     "cpa-plugin-codex-overload-retry retry wrapper",
	})
}

// providerCache caches the codex model set from host.auth.models briefly so
// the router does not issue a host callback per request.
var providerCache struct {
	mu     sync.Mutex
	models map[string]bool
	at     time.Time
}

const providerCacheTTL = 10 * time.Second

func codexProviderServes(sourceFormat, model string) bool {
	if providerModels := cachedCodexModels(); providerModels != nil {
		return modelInSet(providerModels, model)
	}
	// Registry lookup failed; the Responses protocol is the Codex client
	// protocol, so still wrap it as a best-effort heuristic.
	return sourceFormat == "openai-response"
}

func cachedCodexModels() map[string]bool {
	providerCache.mu.Lock()
	defer providerCache.mu.Unlock()
	if providerCache.models != nil && time.Since(providerCache.at) < providerCacheTTL {
		return providerCache.models
	}
	raw, errCall := callHost("host.auth.models", hostAuthModelsRequest{})
	if errCall != nil {
		logHost("warn", "cpa-plugin-codex-overload-retry: host.auth.models lookup failed; falling back to protocol heuristic", map[string]any{
			"error": errCall.Error(),
		})
		providerCache.models = nil
		providerCache.at = time.Now()
		return nil
	}
	var resp hostAuthModelsResponse
	if errUnmarshal := jsonUnmarshal(raw, &resp); errUnmarshal != nil {
		providerCache.models = nil
		providerCache.at = time.Now()
		return nil
	}
	models := make(map[string]bool)
	providers := make(map[string]int)
	for _, cred := range resp.Credentials {
		providerKey := strings.TrimSpace(cred.Provider)
		providers[providerKey] += len(cred.Models)
		if !strings.EqualFold(providerKey, "codex") {
			continue
		}
		for _, m := range cred.Models {
			m = strings.TrimSpace(m)
			if m != "" {
				models[strings.ToLower(m)] = true
				// Also register the base name without any prefix segment.
				if idx := strings.LastIndex(m, "/"); idx >= 0 && idx < len(m)-1 {
					models[strings.ToLower(m[idx+1:])] = true
				}
			}
		}
	}
	if cfg := loadConfig(); cfg.LogRoutes {
		names := make([]string, 0, len(models))
		for m := range models {
			names = append(names, m)
		}
		sort.Strings(names)
		logHost("debug", "cpa-plugin-codex-overload-retry: credential model set refreshed", map[string]any{
			"codexModels":  names,
			"providers":    providers,
		})
	}
	providerCache.models = models
	providerCache.at = time.Now()
	return models
}

func modelInSet(models map[string]bool, model string) bool {
	if models == nil {
		return false
	}
	return models[strings.ToLower(strings.TrimSpace(model))]
}
