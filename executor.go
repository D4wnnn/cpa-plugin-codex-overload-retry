package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	methodHostModelExecute       = "host.model.execute"
	methodHostModelExecuteStream = "host.model.execute_stream"
	methodHostModelStreamRead    = "host.model.stream_read"
	methodHostModelStreamClose   = "host.model.stream_close"
	methodHostStreamEmit         = "host.stream.emit"
	methodHostStreamClose        = "host.stream.close"
)

// ---------------------------------------------------------------------------
// Non-streaming execution
// ---------------------------------------------------------------------------

func executeModel(raw []byte) ([]byte, error) {
	var req executorRequest
	if err := jsonUnmarshal(raw, &req); err != nil {
		return nil, err
	}
	cfg := loadConfig()
	deadline := time.Now().Add(time.Duration(cfg.TotalBudgetSeconds * float64(time.Second)))
	body := req.OriginalRequest
	if len(body) == 0 {
		body = req.Payload
	}
	var lastStatus int
	var lastBody []byte
	attempt := 0
	for {
		attempt++
		rawResult, errCall := callHost(methodHostModelExecute, hostModelExecutionRequest{
			EntryProtocol:  entryFormat(req),
			ExitProtocol:   entryFormat(req),
			Model:          req.Model,
			Stream:         false,
			Body:           body,
			Headers:        req.Headers,
			Query:          req.Query,
			Alt:            req.Alt,
			CallerScope:    callerScopeFrom(req.Metadata),
			HostCallbackID: req.HostCallbackID,
		})
		if errCall != nil {
			status := hostErrorStatus(errCall)
			if canRetry(attempt, cfg, deadline) && cfg.retryStatuses[status] {
				wait := retryAfterFromText(errCall.Error())
				stats.recordRetry(req.Model, attempt, "call error: "+errCall.Error(), wait.String())
				logRetry(cfg, req.Model, attempt, "call error: "+errCall.Error(), wait)
				if !sleepBackoff(attempt, cfg, wait, deadline) {
					break
				}
				continue
			}
			return errorEnvelopeBytes("upstream_error", errCall.Error(), status), nil
		}
		var resp hostModelExecutionResponse
		if errUnmarshal := jsonUnmarshal(rawResult, &resp); errUnmarshal != nil {
			return nil, errUnmarshal
		}
		if resp.StatusCode >= 400 {
			lastStatus, lastBody = resp.StatusCode, resp.Body
			retryable := cfg.retryStatuses[resp.StatusCode] && !deniedBody(resp.Body)
			if retryable && canRetry(attempt, cfg, deadline) {
				wait := maxDuration(retryAfterFromHeader(resp.Headers), retryAfterFromPayload(resp.Body))
				stats.recordRetry(req.Model, attempt, fmt.Sprintf("http %d", resp.StatusCode), wait.String())
				logRetry(cfg, req.Model, attempt, fmt.Sprintf("http %d", resp.StatusCode), wait)
				if !sleepBackoff(attempt, cfg, wait, deadline) {
					break
				}
				continue
			}
			return errorEnvelopeBytes("upstream_error", bodySnippet(resp.Body), resp.StatusCode), nil
		}
		if verdict, _ := scanNonSSEPrelude(resp.Body); verdict == verdictRetry && canRetry(attempt, cfg, deadline) {
			wait := maxDuration(retryAfterFromHeader(resp.Headers), retryAfterFromPayload(resp.Body))
			stats.recordRetry(req.Model, attempt, "in-band overload in 200 body", wait.String())
			logRetry(cfg, req.Model, attempt, "in-band overload in 200 body", wait)
			if !sleepBackoff(attempt, cfg, wait, deadline) {
				break
			}
			continue
		}
		return okEnvelope(executorResponse{Payload: resp.Body, Headers: resp.Headers})
	}
	// Retry budget exhausted.
	stats.recordExhausted(req.Model)
	if lastStatus > 0 && len(lastBody) > 0 {
		return errorEnvelopeBytes("upstream_error", bodySnippet(lastBody), lastStatus), nil
	}
	return errorEnvelopeBytes("retry_exhausted",
		fmt.Sprintf("cpa-plugin-codex-overload-retry: gave up after %d attempts", attempt), http.StatusServiceUnavailable), nil
}

// ---------------------------------------------------------------------------
// Streaming execution
// ---------------------------------------------------------------------------

func executeModelStream(raw []byte) ([]byte, error) {
	var req executorRequest
	if err := jsonUnmarshal(raw, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.StreamID) == "" {
		return errorEnvelopeBytes("executor_error", "stream_id is required for executor.execute_stream", 0), nil
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				closePluginStream(req.StreamID, fmt.Sprintf("stream orchestration panic: %v", recovered))
			}
		}()
		if err := orchestrateStream(req, loadConfig()); err != nil {
			closePluginStream(req.StreamID, err.Error())
			return
		}
		closePluginStream(req.StreamID, "")
	}()
	return okEnvelope(executorStreamResponse{
		Headers: http.Header{"Content-Type": []string{"text/event-stream"}},
	})
}

type attemptOutcome int

const (
	outcomeCommitted attemptOutcome = iota
	outcomeRetry
	outcomeFatal
)

type streamAttemptResult struct {
	outcome    attemptOutcome
	reason     string
	retryAfter time.Duration
	err        error
}

// orchestrateStream drives the retry loop for one client stream.
func orchestrateStream(req executorRequest, cfg pluginConfig) error {
	deadline := time.Now().Add(time.Duration(cfg.TotalBudgetSeconds * float64(time.Second)))
	var lastReason string
	attempt := 0
	for {
		attempt++
		result := runStreamAttempt(req, cfg, deadline)
		switch result.outcome {
		case outcomeCommitted:
			return nil
		case outcomeFatal:
			stats.recordFatal(req.Model)
			return result.err
		case outcomeRetry:
			lastReason = result.reason
			if !canRetry(attempt, cfg, deadline) {
				stats.recordExhausted(req.Model)
				return exhaustedStream(req, cfg, attempt, lastReason)
			}
			stats.recordRetry(req.Model, attempt, result.reason, result.retryAfter.String())
			logRetry(cfg, req.Model, attempt, result.reason, result.retryAfter)
			if !sleepBackoff(attempt, cfg, result.retryAfter, deadline) {
				stats.recordExhausted(req.Model)
				return exhaustedStream(req, cfg, attempt, lastReason)
			}
		}
	}
}

// runStreamAttempt performs one nested host model execution and either
// forwards the stream to the client (committed) or reports a retryable
// rejection. Chunks are emitted only after the stream is committed.
func runStreamAttempt(req executorRequest, cfg pluginConfig, deadline time.Time) streamAttemptResult {
	body := req.OriginalRequest
	if len(body) == 0 {
		body = req.Payload
	}
	rawResult, errCall := callHost(methodHostModelExecuteStream, hostModelExecutionRequest{
		EntryProtocol:  entryFormat(req),
		ExitProtocol:   entryFormat(req),
		Model:          req.Model,
		Stream:         true,
		Body:           body,
		Headers:        req.Headers,
		Query:          req.Query,
		Alt:            req.Alt,
		CallerScope:    callerScopeFrom(req.Metadata),
		HostCallbackID: req.HostCallbackID,
	})
	if errCall != nil {
		status := hostErrorStatus(errCall)
		if cfg.retryStatuses[status] {
			return streamAttemptResult{outcome: outcomeRetry, reason: "call error: " + errCall.Error(), retryAfter: retryAfterFromText(errCall.Error())}
		}
		return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: host model execution failed: %w", errCall)}
	}
	var resp hostModelStreamResponse
	if errUnmarshal := jsonUnmarshal(rawResult, &resp); errUnmarshal != nil {
		return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: decode host stream response: %w", errUnmarshal)}
	}
	retryAfter := retryAfterFromHeader(resp.Headers)

	if resp.StatusCode >= 400 {
		if cfg.retryStatuses[resp.StatusCode] {
			// Retryable upstream status: discard the error body and restart.
			closeHostModelStream(resp.StreamID)
			return streamAttemptResult{outcome: outcomeRetry, reason: fmt.Sprintf("http %d", resp.StatusCode), retryAfter: retryAfter}
		}
		// Non-retryable upstream status: forward the body as stream content
		// so the client sees the real upstream error.
		if resp.StreamID != "" {
			drainAndPassThrough(req, resp.StreamID)
			closeHostModelStream(resp.StreamID)
		}
		return streamAttemptResult{outcome: outcomeCommitted}
	}
	if strings.TrimSpace(resp.StreamID) == "" {
		return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: host model stream returned an empty stream_id")}
	}

	// Read the nested stream, buffering the head until commit.
	frames := &sseFrames{}
	var prelude [][]byte
	preludeSize := 0
	committed := false
	// Max time to hold the stream head while deciding whether to retry.
	attemptDeadline := time.Now().Add(time.Duration(cfg.CommitDeadlineSeconds * float64(time.Second)))
	if remaining := deadline.Sub(attemptDeadline); remaining < 0 {
		attemptDeadline = deadline
	}
	commit := func() error {
		committed = true
		for _, p := range prelude {
			if errEmit := emitChunk(req.StreamID, p); errEmit != nil {
				return errEmit
			}
		}
		prelude = nil
		return nil
	}
	for {
		chunkRaw, errRead := callHost(methodHostModelStreamRead, hostModelStreamReadRequest{StreamID: resp.StreamID})
		if errRead != nil {
			closeHostModelStream(resp.StreamID)
			if committed {
				return streamAttemptResult{outcome: outcomeCommitted}
			}
			return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: stream read failed: %w", errRead)}
		}
		var chunk hostModelStreamReadResponse
		if errUnmarshal := jsonUnmarshal(chunkRaw, &chunk); errUnmarshal != nil {
			closeHostModelStream(resp.StreamID)
			return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: decode stream chunk: %w", errUnmarshal)}
		}
		if len(chunk.Payload) > 0 {
			if committed {
				if errEmit := emitChunk(req.StreamID, chunk.Payload); errEmit != nil {
					closeHostModelStream(resp.StreamID)
					return streamAttemptResult{outcome: outcomeCommitted}
				}
			} else {
				prelude = append(prelude, chunk.Payload)
				preludeSize += len(chunk.Payload)
				events := frames.feed(chunk.Payload)
				for _, ev := range events {
					verdict, info := classifyEvent(ev)
					if verdict == verdictRetry {
						closeHostModelStream(resp.StreamID)
						return streamAttemptResult{outcome: outcomeRetry, reason: info, retryAfter: maxDuration(retryAfter, retryAfterFromPayload(ev))}
					}
					if verdict == verdictCommit {
						if errCommit := commit(); errCommit != nil {
							closeHostModelStream(resp.StreamID)
							return streamAttemptResult{outcome: outcomeCommitted}
						}
						break
					}
				}
				if !committed && len(events) == 0 {
					if verdict, _ := scanNonSSEPrelude(bytes.Join(prelude, nil)); verdict == verdictRetry {
						closeHostModelStream(resp.StreamID)
						return streamAttemptResult{outcome: outcomeRetry, reason: "raw marker", retryAfter: retryAfter}
					}
				}
				if !committed && (preludeSize >= cfg.PreludeLimitBytes || time.Now().After(attemptDeadline)) {
					if errCommit := commit(); errCommit != nil {
						closeHostModelStream(resp.StreamID)
						return streamAttemptResult{outcome: outcomeCommitted}
					}
				}
			}
		}
		if chunk.Error != "" {
			closeHostModelStream(resp.StreamID)
			if !committed && isRetryableErrorText(chunk.Error) {
				return streamAttemptResult{outcome: outcomeRetry, reason: "stream error (overload): " + chunk.Error, retryAfter: maxDuration(retryAfter, retryAfterFromText(chunk.Error))}
			}
			// Surface post-commit and non-retryable stream errors; the
			// client decides how to recover (same policy as the reference).
			return streamAttemptResult{outcome: outcomeFatal, err: fmt.Errorf("cpa-plugin-codex-overload-retry: upstream stream error: %s", chunk.Error)}
		}
		if chunk.Done {
			closeHostModelStream(resp.StreamID)
			if committed {
				return streamAttemptResult{outcome: outcomeCommitted}
			}
			// EOF before output: pass the buffered bytes through untouched.
			if errCommit := commit(); errCommit != nil {
				return streamAttemptResult{outcome: outcomeCommitted}
			}
			return streamAttemptResult{outcome: outcomeCommitted}
		}
	}
}

// drainAndPassThrough forwards every remaining chunk of a nested stream to
// the client stream without classification (used for non-retryable statuses).
func drainAndPassThrough(req executorRequest, streamID string) {
	for {
		chunkRaw, errRead := callHost(methodHostModelStreamRead, hostModelStreamReadRequest{StreamID: streamID})
		if errRead != nil {
			return
		}
		var chunk hostModelStreamReadResponse
		if errUnmarshal := jsonUnmarshal(chunkRaw, &chunk); errUnmarshal != nil {
			return
		}
		if len(chunk.Payload) > 0 {
			if errEmit := emitChunk(req.StreamID, chunk.Payload); errEmit != nil {
				return
			}
		}
		if chunk.Error != "" || chunk.Done {
			return
		}
	}
}

// exhaustedStream is the terminal path when the retry budget is spent:
// emit a synthetic in-band error event so the client still gets a
// well-formed stream.
func exhaustedStream(req executorRequest, cfg pluginConfig, attempts int, reason string) error {
	payload := syntheticExhaustedEvent(req, attempts, reason)
	if errEmit := emitChunk(req.StreamID, payload); errEmit != nil {
		return errEmit
	}
	return nil
}

func syntheticExhaustedEvent(req executorRequest, attempts int, reason string) []byte {
	message := fmt.Sprintf("cpa-plugin-codex-overload-retry: gave up after %d attempts: %s", attempts, reason)
	var doc map[string]any
	if strings.EqualFold(entryFormat(req), "openai") {
		doc = map[string]any{
			"error": map[string]any{
				"message": message,
				"type":    "server_error",
				"code":    "server_is_overloaded",
			},
		}
	} else {
		doc = map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "service_unavailable_error",
				"code":    "server_is_overloaded",
				"message": message,
			},
		}
	}
	raw, _ := json.Marshal(doc)
	out := make([]byte, 0, len(raw)+8)
	out = append(out, "data: "...)
	out = append(out, raw...)
	out = append(out, '\n', '\n')
	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func entryFormat(req executorRequest) string {
	if f := strings.ToLower(strings.TrimSpace(req.SourceFormat)); f != "" {
		return f
	}
	return strings.ToLower(strings.TrimSpace(req.Format))
}

func callerScopeFrom(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v, ok := metadata["caller_scope"].(string); ok {
		return v
	}
	return ""
}

func canRetry(attempt int, cfg pluginConfig, deadline time.Time) bool {
	return attempt < cfg.MaxAttempts && time.Now().Before(deadline)
}

// sleepBackoff waits for the computed backoff delay; returns false when the
// total deadline expires first.
func sleepBackoff(attempt int, cfg pluginConfig, retryAfter time.Duration, deadline time.Time) bool {
	delay := backoffDelay(attempt, cfg, retryAfter)
	limit := deadline.Sub(time.Now())
	if limit <= 0 {
		return false
	}
	if delay > limit {
		delay = limit
	}
	wakeAt := time.Now().Add(delay)
	for time.Now().Before(wakeAt) {
		remaining := time.Until(wakeAt)
		if remaining > 100*time.Millisecond {
			remaining = 100 * time.Millisecond
		}
		time.Sleep(remaining)
	}
	return time.Now().Before(deadline)
}

func backoffDelay(attempt int, cfg pluginConfig, retryAfter time.Duration) time.Duration {
	base := cfg.BackoffBaseSeconds
	if base <= 0 {
		base = 1
	}
	factor := cfg.BackoffFactor
	if factor <= 1 {
		factor = 1.6
	}
	delay := base * math.Pow(factor, float64(attempt-1))
	maxDelay := cfg.BackoffMaxSeconds
	if maxDelay <= 0 {
		maxDelay = 8
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	delay *= 0.7 + 0.6*rand.Float64()
	d := time.Duration(delay * float64(time.Second))
	// Honor upstream retry-after guidance (from headers or embedded in the
	// error payload), capped by max_retry_wait_seconds.
	if retryAfter > d {
		d = retryAfter
	}
	maxWait := cfg.MaxRetryWaitSeconds
	if maxWait <= 0 {
		maxWait = 300
	}
	if capDelay := time.Duration(maxWait) * time.Second; d > capDelay {
		d = capDelay
	}
	return d
}

func retryAfterFromHeader(headers http.Header) time.Duration {
	if headers == nil {
		return 0
	}
	value := strings.TrimSpace(headers.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(math.Min(seconds, 86400) * float64(time.Second))
	}
	if t, err := http.ParseTime(value); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		if d > 86400*time.Second {
			d = 86400 * time.Second
		}
		return d
	}
	return 0
}

// ---------------------------------------------------------------------------
// Upstream retry-after guidance
// ---------------------------------------------------------------------------

// Codex upstreams embed retry guidance in the error payload itself:
//
//	{"error":{"code":"server_is_overloaded","headers":{"retry-after":"304","retry-after-ms":"304000"},...}}
//
// These helpers extract that guidance so backoff honors it instead of
// hammering an upstream that explicitly said "wait 5 minutes".

var (
	retryAfterMSRe = regexp.MustCompile(`"retry-after-ms":\s*"?(\d+)(")?`)
	retryAfterSecRe = regexp.MustCompile(`"retry-after":\s*"?(\d+)(")?`)
)

func clampDuration(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	if d > 24*time.Hour {
		return 24 * time.Hour
	}
	return d
}

func retryAfterFromHeadersMap(headers map[string]any) time.Duration {
	if headers == nil {
		return 0
	}
	if ms, ok := anyToFloat(headers["retry-after-ms"]); ok && ms > 0 {
		return clampDuration(time.Duration(ms * float64(time.Millisecond)))
	}
	if seconds, ok := anyToFloat(headers["retry-after"]); ok && seconds > 0 {
		return clampDuration(time.Duration(seconds * float64(time.Second)))
	}
	return 0
}

func anyToFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	case float64:
		return t, true
	case int:
		return float64(t), true
	}
	return 0, false
}

// retryAfterFromPayload extracts retry guidance from a JSON error payload.
func retryAfterFromPayload(raw []byte) time.Duration {
	var doc struct {
		Error *struct {
			Headers map[string]any `json:"headers"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil || doc.Error == nil {
		return 0
	}
	return retryAfterFromHeadersMap(doc.Error.Headers)
}

// retryAfterFromText extracts retry guidance from an error string that
// embeds an upstream JSON error (or fragments of one).
func retryAfterFromText(text string) time.Duration {
	if v := retryAfterFromPayload([]byte(text)); v > 0 {
		return v
	}
	if m := retryAfterMSRe.FindStringSubmatch(text); m != nil {
		if ms, err := strconv.ParseFloat(m[1], 64); err == nil && ms > 0 {
			return clampDuration(time.Duration(ms * float64(time.Millisecond)))
		}
	}
	if m := retryAfterSecRe.FindStringSubmatch(text); m != nil {
		if seconds, err := strconv.ParseFloat(m[1], 64); err == nil && seconds > 0 {
			return clampDuration(time.Duration(seconds * float64(time.Second)))
		}
	}
	return 0
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func deniedBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil || doc == nil {
		return false
	}
	if errObj, ok := doc["error"].(map[string]any); ok {
		return isDeniedError(errObj)
	}
	return false
}

func bodySnippet(body []byte) string {
	const limit = 512
	text := strings.TrimSpace(string(body))
	if len(text) > limit {
		text = text[:limit] + "..."
	}
	return text
}

func hostErrorStatus(err error) int {
	if he, ok := err.(*hostError); ok && he.httpStatus > 0 {
		return he.httpStatus
	}
	if err == nil {
		return 0
	}
	msg := err.Error()
	for _, code := range []int{429, 500, 502, 503, 504} {
		if strings.Contains(msg, strconv.Itoa(code)) {
			return code
		}
	}
	return 0
}

func emitChunk(streamID string, payload []byte) error {
	if strings.TrimSpace(streamID) == "" {
		return fmt.Errorf("plugin stream id is required")
	}
	_, errCall := callHost(methodHostStreamEmit, streamEmitRequest{
		StreamID: streamID,
		Payload:  payload,
	})
	return errCall
}

func closePluginStream(streamID, errMsg string) {
	if strings.TrimSpace(streamID) == "" {
		return
	}
	_, _ = callHost(methodHostStreamClose, streamCloseRequest{
		StreamID: streamID,
		Error:    strings.TrimSpace(errMsg),
	})
}

func closeHostModelStream(streamID string) {
	if strings.TrimSpace(streamID) == "" {
		return
	}
	_, _ = callHost(methodHostModelStreamClose, hostModelStreamCloseRequest{StreamID: streamID})
}

func logRetry(cfg pluginConfig, model string, attempt int, reason string, retryAfter time.Duration) {
	if !cfg.LogRetries {
		return
	}
	logHost("info", fmt.Sprintf("cpa-plugin-codex-overload-retry: absorbing retry %d for %s", attempt, model), map[string]any{
		"model":       model,
		"attempt":     attempt,
		"reason":      reason,
		"retry_after": retryAfter.String(),
	})
}
