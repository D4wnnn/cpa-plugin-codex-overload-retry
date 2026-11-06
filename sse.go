package main

// SSE framing and in-band overload classification, ported from the reference
// implementation relay/relay.py (cpa-plugin-codex-overload-retry by NewBeeM4n, MIT).

import (
	"bytes"
	"encoding/json"
	"strings"
)

// overloadCodes matches error.code / error.type values that indicate a retryable
// upstream capacity rejection.
var overloadCodes = map[string]bool{
	"server_overloaded":         true, // response.failed / response.error
	"server_is_overloaded":      true, // bare {"type":"error"} event (observed on real traffic)
	"service_unavailable_error": true,
	"flex_unavailable":          true,
	"queue_full":                true,
}

// deniedWords mark quota/billing/auth failures that must NOT be retried.
var deniedWords = []string{"quota", "billing", "insufficient", "authentication", "unauthorized", "invalid_api_key"}

// overloadMarkers are raw text markers used as a fallback scan for payloads
// that are not framed as SSE events (whole JSON bodies, translated formats).
var overloadMarkers = []string{
	"server_overloaded",
	"server_is_overloaded",
	"service_unavailable_error",
	"flex_unavailable",
	"queue_full",
	"at capacity",
	"overloaded",
}

func errFieldText(err map[string]any) string {
	var parts []string
	for _, key := range []string{"code", "type", "message"} {
		if v, ok := err[key].(string); ok {
			parts = append(parts, strings.ToLower(v))
		}
	}
	return strings.Join(parts, " ")
}

func isDeniedError(err map[string]any) bool {
	if err == nil {
		return false
	}
	text := errFieldText(err)
	return containsAnyWord(text, deniedWords)
}

func isOverloadError(err map[string]any) bool {
	if err == nil || isDeniedError(err) {
		return false
	}
	code, _ := err["code"].(string)
	etype, _ := err["type"].(string)
	msg, _ := err["message"].(string)
	if overloadCodes[code] || overloadCodes[etype] {
		return true
	}
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "at capacity") || strings.Contains(msg, "overloaded")
}

func containsAnyWord(text string, words []string) bool {
	for _, w := range words {
		if strings.Contains(text, w) {
			return true
		}
	}
	return false
}

// isRetryableErrorText reports whether a raw error string looks like a
// retryable overload failure (and not a denied failure).
func isRetryableErrorText(text string) bool {
	t := strings.ToLower(text)
	if containsAnyWord(t, deniedWords) {
		return false
	}
	return containsAnyWord(t, overloadMarkers)
}

// sseFrames frames multiline SSE data across chunks, including CR, LF and CRLF.
type sseFrames struct {
	line    []byte
	data    [][]byte
	afterCR bool
}

// feed consumes one chunk and returns completed SSE event payloads
// (multi-line data joined with "\n"), possibly none.
func (f *sseFrames) feed(chunk []byte) [][]byte {
	var events [][]byte
	for _, c := range chunk {
		if f.afterCR {
			f.afterCR = false
			if c == '\n' {
				continue
			}
		}
		if c == '\n' || c == '\r' {
			line := f.line
			f.line = f.line[:0]
			f.afterCR = c == '\r'
			if len(line) == 0 {
				if len(f.data) > 0 {
					payload := bytes.Join(f.data, []byte("\n"))
					f.data = f.data[:0]
					if len(payload) > 0 {
						events = append(events, payload)
					}
				}
			} else if bytes.HasPrefix(line, []byte("data:")) {
				value := line[5:]
				if len(value) > 0 && value[0] == ' ' {
					value = value[1:]
				}
				f.data = append(f.data, append([]byte(nil), value...))
			}
		} else {
			f.line = append(f.line, c)
		}
	}
	return events
}

const (
	verdictBuffer = ""      // known empty setup event; keep buffering
	verdictCommit = "commit" // output or unknown event; stop buffering, forward
	verdictRetry  = "retry"  // in-band overload error; restart the attempt
)

// classifyEvent inspects one SSE event payload.
// Only known empty setup events may be buffered; unknown output commits.
func classifyEvent(payload []byte) (verdict string, info string) {
	var evt map[string]any
	if err := json.Unmarshal(payload, &evt); err != nil || evt == nil {
		return verdictCommit, "unknown event"
	}
	kind, _ := evt["type"].(string)
	response, _ := evt["response"].(map[string]any)
	if response == nil {
		response = map[string]any{}
	}
	errObj, _ := response["error"].(map[string]any)
	if errObj == nil {
		errObj, _ = evt["error"].(map[string]any)
	}
	switch kind {
	case "error", "response.error", "response.failed":
		if errObj == nil {
			errObj = evt
		}
		if isOverloadError(errObj) {
			return verdictRetry, "overload"
		}
		return verdictCommit, "error event"
	case "response.created", "response.in_progress", "response.queued", "response.metadata", "ping":
		if _, hasOutput := response["output"]; hasOutput && !isEmptyJSONValue(response["output"]) {
			return verdictCommit, "lifecycle output"
		}
		return verdictBuffer, ""
	}
	// OpenAI chat-completions style top-level error object.
	if kind == "" && errObj != nil {
		if isOverloadError(errObj) {
			return verdictRetry, "overload"
		}
		return verdictCommit, "error object"
	}
	if item, ok := evt["item"].(map[string]any); ok && kind == "response.output_item.added" &&
		item["type"] == "message" && item["role"] == "assistant" && isEmptyJSONValue(item["content"]) &&
		itemKeysWithin(item, "id", "type", "role", "status", "content", "phase") {
		return verdictBuffer, ""
	}
	return verdictCommit, "output event"
}

func isEmptyJSONValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case []any:
		return len(t) == 0
	case string:
		return t == ""
	default:
		return false
	}
}

func itemKeysWithin(item map[string]any, allowed ...string) bool {
	allowedSet := make(map[string]bool, len(allowed))
	for _, k := range allowed {
		allowedSet[k] = true
	}
	for k := range item {
		if !allowedSet[k] {
			return false
		}
	}
	return true
}

// scanNonSSEPrelude classifies a buffered body that does not look like SSE
// (e.g. a whole JSON response body delivered as stream chunks).
func scanNonSSEPrelude(prelude []byte) (verdict string, info string) {
	trimmed := bytes.TrimSpace(prelude)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var doc map[string]any
		if err := json.Unmarshal(trimmed, &doc); err == nil && doc != nil {
			if errObj, ok := doc["error"].(map[string]any); ok {
				if isOverloadError(errObj) {
					return verdictRetry, "overload"
				}
				return verdictCommit, "error object"
			}
			if kind, _ := doc["type"].(string); kind == "error" {
				if isOverloadError(doc) {
					return verdictRetry, "overload"
				}
				return verdictCommit, "error event"
			}
			return verdictCommit, "json body"
		}
	}
	lower := strings.ToLower(string(prelude))
	if containsAnyWord(lower, deniedWords) {
		return verdictBuffer, ""
	}
	if containsAnyWord(lower, overloadMarkers) {
		return verdictRetry, "raw marker"
	}
	return verdictBuffer, ""
}
