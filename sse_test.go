package main

import (
	"testing"
	"time"
)

func TestFeedFramesMultilineCRLF(t *testing.T) {
	f := &sseFrames{}
	var events [][]byte
	events = append(events, f.feed([]byte("data: {\"a\":"))...)
	events = append(events, f.feed([]byte("1}\r\n"))...)
	events = append(events, f.feed([]byte("\r\ndata: {\"b\":2}\n\n"))...)
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2: %q", len(events), events)
	}
	if string(events[0]) != `{"a":1}` {
		t.Fatalf("event0 = %q", events[0])
	}
	if string(events[1]) != `{"b":2}` {
		t.Fatalf("event1 = %q", events[1])
	}
}

func TestFeedFramesLoneCR(t *testing.T) {
	f := &sseFrames{}
	events := f.feed([]byte("data: x\rdata: y\r\r"))
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if string(events[0]) != "x\ny" {
		t.Fatalf("event = %q, want x\\ny", events[0])
	}
}

func TestClassifyOverloadError(t *testing.T) {
	// The exact event shape observed on real traffic.
	payload := []byte(`{"type":"error","sequence_number":3,"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`)
	verdict, info := classifyEvent(payload)
	if verdict != verdictRetry {
		t.Fatalf("verdict = %q (%s), want retry", verdict, info)
	}
}

func TestClassifyResponseFailedOverload(t *testing.T) {
	payload := []byte(`{"type":"response.failed","response":{"error":{"code":"server_overloaded","message":"overloaded"}}}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictRetry {
		t.Fatalf("verdict = %q, want retry", verdict)
	}
}

func TestClassifyDeniedQuotaCommits(t *testing.T) {
	payload := []byte(`{"type":"response.failed","response":{"error":{"code":"insufficient_quota","message":"You exceeded your quota"}}}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictCommit {
		t.Fatalf("verdict = %q, want commit", verdict)
	}
}

func TestClassifyLifecycleBuffered(t *testing.T) {
	for _, kind := range []string{"response.created", "response.in_progress", "response.queued", "response.metadata", "ping"} {
		payload := []byte(`{"type":"` + kind + `","response":{"id":"resp_1"}}`)
		verdict, _ := classifyEvent(payload)
		if verdict != verdictBuffer {
			t.Fatalf("%s: verdict = %q, want buffer", kind, verdict)
		}
	}
}

func TestClassifyLifecycleWithOutputCommits(t *testing.T) {
	payload := []byte(`{"type":"response.in_progress","response":{"output":[{"type":"message"}]}}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictCommit {
		t.Fatalf("verdict = %q, want commit", verdict)
	}
}

func TestClassifyEmptyAssistantItemBuffered(t *testing.T) {
	payload := []byte(`{"type":"response.output_item.added","item":{"id":"item_1","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictBuffer {
		t.Fatalf("verdict = %q, want buffer", verdict)
	}
}

func TestClassifyOutputTextCommits(t *testing.T) {
	payload := []byte(`{"type":"response.output_text.delta","delta":"hi"}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictCommit {
		t.Fatalf("verdict = %q, want commit", verdict)
	}
}

func TestClassifyChatCompletionsError(t *testing.T) {
	payload := []byte(`{"error":{"message":"The model is at capacity. Please try again.","type":"server_error","code":"server_is_overloaded"}}`)
	verdict, _ := classifyEvent(payload)
	if verdict != verdictRetry {
		t.Fatalf("verdict = %q, want retry", verdict)
	}
}

func TestClassifyInvalidJSONCommits(t *testing.T) {
	verdict, _ := classifyEvent([]byte("not json"))
	if verdict != verdictCommit {
		t.Fatalf("verdict = %q, want commit", verdict)
	}
}

func TestScanNonSSEJSONErrorBody(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded."}}`)
	verdict, _ := scanNonSSEPrelude(body)
	if verdict != verdictRetry {
		t.Fatalf("verdict = %q, want retry", verdict)
	}
}

func TestScanNonSSEJSONSuccessBody(t *testing.T) {
	body := []byte(`{"id":"resp_1","object":"response","status":"completed","output":[]}`)
	verdict, _ := scanNonSSEPrelude(body)
	if verdict != verdictCommit {
		t.Fatalf("verdict = %q, want commit", verdict)
	}
}

func TestScanNonSSEPartialBodyBuffers(t *testing.T) {
	verdict, _ := scanNonSSEPrelude([]byte(`{"id":"resp_1","obj`))
	if verdict != verdictBuffer {
		t.Fatalf("verdict = %q, want buffer", verdict)
	}
}

func TestIsRetryableErrorText(t *testing.T) {
	if !isRetryableErrorText("server_is_overloaded: model at capacity") {
		t.Fatal("overload text should be retryable")
	}
	if isRetryableErrorText("insufficient_quota: billing hard limit reached") {
		t.Fatal("quota text must not be retryable")
	}
	if isRetryableErrorText("invalid request") {
		t.Fatal("neutral text must not be retryable")
	}
}

func TestBackoffDelayBounds(t *testing.T) {
	cfg := defaultConfig()
	d := backoffDelay(1, cfg, 0)
	if d <= 0 {
		t.Fatalf("delay = %v, want > 0", d)
	}
	d = backoffDelay(50, cfg, 0)
	maxCap := time.Duration(cfg.MaxRetryWaitSeconds) * time.Second
	if d > maxCap+time.Second {
		t.Fatalf("delay = %v, exceeds cap %v", d, maxCap)
	}
	d = backoffDelay(2, cfg, 2*time.Hour)
	if d != maxCap {
		t.Fatalf("delay = %v, want capped at %v", d, maxCap)
	}
}

func TestRetryAfterFromPayload(t *testing.T) {
	// The exact shape observed on real codex upstream traffic.
	payload := []byte(`{"error":{"type":"service_unavailable_error","code":"server_is_overloaded","headers":{"x-retry-metadata":"NO_MORE_RETRY","retry-after":"304","retry-after-ms":"304000"},"message":"Our servers are currently overloaded."}}`)
	if d := retryAfterFromPayload(payload); d != 304*time.Second {
		t.Fatalf("retryAfter = %v, want 304s", d)
	}
	if d := retryAfterFromPayload([]byte(`{"ok":true}`)); d != 0 {
		t.Fatalf("retryAfter = %v, want 0", d)
	}
}

func TestRetryAfterFromText(t *testing.T) {
	d := retryAfterFromText(`call failed: {"error":{"headers":{"retry-after-ms":"120000"}}}`)
	if d != 120*time.Second {
		t.Fatalf("retryAfter = %v, want 120s", d)
	}
	d = retryAfterFromText(`upstream said "retry-after":"42" please wait`)
	if d != 42*time.Second {
		t.Fatalf("retryAfter = %v, want 42s", d)
	}
	if d := retryAfterFromText("no guidance here"); d != 0 {
		t.Fatalf("retryAfter = %v, want 0", d)
	}
}
