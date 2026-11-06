package main

import (
	"encoding/json"
	"net/http"
	"net/url"
)

// RPC mirrors of CLIProxyAPI plugin host schemas (v8.0.52).
// Field names/tags must match how the host marshals its rpc* structs.
// sdk types without json tags marshal with Go field names (encoding/json default).

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

type hostError struct {
	code       string
	message    string
	httpStatus int
}

func (e *hostError) Error() string {
	if e == nil {
		return ""
	}
	if e.code != "" {
		return e.code + ": " + e.message
	}
	return e.message
}

// lifecycleRequest mirrors pluginhost.rpcLifecycleRequest.
type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

// registration mirrors pluginhost.rpcRegistration.
type registration struct {
	SchemaVersion uint32       `json:"schema_version"`
	Metadata      metadata     `json:"metadata"`
	Capabilities  capabilities `json:"capabilities"`
}

type metadata struct {
	Name             string        `json:"Name"`
	Version          string        `json:"Version"`
	Author           string        `json:"Author"`
	GitHubRepository string        `json:"GitHubRepository"`
	Logo             string        `json:"Logo"`
	ConfigFields     []configField `json:"ConfigFields"`
}

type configField struct {
	Name        string   `json:"Name"`
	Type        string   `json:"Type"`
	Default     any      `json:"Default,omitempty"`
	EnumValues  []string `json:"EnumValues,omitempty"`
	Description string   `json:"Description"`
}

type capabilities struct {
	ModelRouter           bool     `json:"model_router"`
	Executor              bool     `json:"executor"`
	ExecutorModelScope    string   `json:"executor_model_scope"`
	ExecutorInputFormats  []string `json:"executor_input_formats"`
	ExecutorOutputFormats []string `json:"executor_output_formats"`
	ManagementAPI         bool     `json:"management_api"`
}

// modelRouteRequest mirrors pluginhost.rpcModelRouteRequest
// (embedded pluginapi.ModelRouteRequest fields marshal with Go names).
type modelRouteRequest struct {
	SourceFormat       string         `json:"SourceFormat"`
	RequestedModel     string         `json:"RequestedModel"`
	Stream             bool           `json:"Stream"`
	Headers            http.Header    `json:"Headers"`
	Query              url.Values     `json:"Query"`
	Body               []byte         `json:"Body"`
	Metadata           map[string]any `json:"Metadata"`
	PluginID           string         `json:"PluginID"`
	AvailableProviders []string       `json:"AvailableProviders"`
	HostCallbackID     string         `json:"host_callback_id,omitempty"`
}

// modelRouteResponse mirrors pluginapi.ModelRouteResponse (Go names, no tags).
type modelRouteResponse struct {
	Handled     bool   `json:"Handled"`
	TargetKind  string `json:"TargetKind"`
	Target      string `json:"Target,omitempty"`
	TargetModel string `json:"TargetModel,omitempty"`
	Reason      string `json:"Reason,omitempty"`
}

// executorRequest mirrors pluginhost.rpcExecutorRequest
// (embedded pluginapi.ExecutorRequest fields marshal with Go names).
type executorRequest struct {
	AuthID          string         `json:"AuthID"`
	AuthProvider    string         `json:"AuthProvider"`
	Model           string         `json:"Model"`
	Format          string         `json:"Format"`
	Stream          bool           `json:"Stream"`
	Alt             string         `json:"Alt"`
	Headers         http.Header    `json:"Headers"`
	Query           url.Values     `json:"Query"`
	OriginalRequest []byte         `json:"OriginalRequest"`
	SourceFormat    string         `json:"SourceFormat"`
	Payload         []byte         `json:"Payload"`
	Metadata        map[string]any `json:"Metadata"`
	StorageJSON     []byte         `json:"StorageJSON"`
	StreamID        string         `json:"stream_id,omitempty"`
	HostCallbackID  string         `json:"host_callback_id,omitempty"`
}

// executorResponse mirrors pluginapi.ExecutorResponse (Go names).
type executorResponse struct {
	Payload  []byte         `json:"Payload"`
	Headers  http.Header    `json:"Headers,omitempty"`
	Metadata map[string]any `json:"Metadata,omitempty"`
}

// executorStreamResponse mirrors pluginhost.rpcExecutorStreamResponse (lowercase tags).
type executorStreamResponse struct {
	Headers http.Header `json:"headers,omitempty"`
	Chunks  []streamChk `json:"chunks,omitempty"`
}

type streamChk struct {
	Payload []byte `json:"Payload,omitempty"`
}

type identifierResponse struct {
	Identifier string `json:"identifier"`
}

// Host callback payloads. pluginapi types here carry json tags; mirror them.

type hostModelExecutionRequest struct {
	EntryProtocol  string      `json:"entry_protocol"`
	ExitProtocol   string      `json:"exit_protocol"`
	Model          string      `json:"model"`
	Stream         bool        `json:"stream"`
	Body           []byte      `json:"body"`
	Headers        http.Header `json:"headers"`
	Query          url.Values  `json:"query"`
	Alt            string      `json:"alt"`
	CallerScope    string      `json:"caller_scope,omitempty"`
	HostCallbackID string      `json:"host_callback_id,omitempty"`
}

type hostModelExecutionResponse struct {
	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers"`
	Body       []byte      `json:"body"`
}

type hostModelStreamResponse struct {
	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers"`
	StreamID   string      `json:"stream_id"`
}

type hostModelStreamReadRequest struct {
	StreamID string `json:"stream_id"`
}

type hostModelStreamReadResponse struct {
	Payload []byte `json:"payload"`
	Error   string `json:"error"`
	Done    bool   `json:"done"`
}

type hostModelStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
}

type hostAuthModelsRequest struct {
	AuthIndex string `json:"auth_index,omitempty"`
}

type hostAuthModelsResponse struct {
	Credentials []hostAuthModels `json:"credentials"`
}

type hostAuthModels struct {
	Provider string   `json:"provider"`
	Models   []string `json:"models"`
}

type streamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
}

type streamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

type hostLogRequest struct {
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}
