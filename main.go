package main

// cpa-plugin-codex-overload-retry: a CLIProxyAPI native plugin that recognizes "Selected
// model is at capacity" style in-band overload errors in codex upstream
// streams and retries them before any output reaches the client.
//
// Detection logic ported from https://github.com/NewBeeM4n/codex-overload-fix
// (relay/relay.py, MIT), adapted to the CLIProxyAPI plugin C ABI:
//
//   - ModelRouter routes matching requests to this plugin's executor.
//   - The executor re-enters the host pipeline through host.model.execute[_stream]
//     with the originating host_callback_id, so the host skips this plugin's
//     router/interceptors on the nested call (no recursion).
//   - The stream head is buffered and classified; retryable overload errors
//     restart the attempt with backoff, real output commits the stream.

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"unsafe"
)

const abiVersion uint32 = 1

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(abiVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelopeBytes("invalid_method", "method is required", 0))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelopeBytes("plugin_error", errHandle.Error(), 0))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if errConfigure := configure(request); errConfigure != nil {
			return nil, errConfigure
		}
		return okEnvelope(pluginRegistration(loadConfig()))
	case "plugin.quiesce", "plugin.shutdown":
		return okEnvelope(map[string]any{})
	case "model.route":
		return routeModel(request)
	case "executor.identifier":
		return okEnvelope(identifierResponse{Identifier: pluginName})
	case "executor.execute":
		return executeModel(request)
	case "executor.execute_stream":
		return executeModelStream(request)
	case "executor.count_tokens":
		return errorEnvelopeBytes("unsupported", "cpa-plugin-codex-overload-retry does not implement count_tokens", 0), nil
	case "management.register":
		return registerManagement()
	case "management.handle":
		return handleManagement(request)
	default:
		return errorEnvelopeBytes("unknown_method", "unknown method: "+method, 0), nil
	}
}

// ---------------------------------------------------------------------------
// Host callback bridge
// ---------------------------------------------------------------------------

// callHost invokes a host.* RPC method through the stored C host API and
// decodes the envelope.
func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal host callback payload %s: %w", method, errMarshal)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		cPayload := C.CBytes(rawPayload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback payload %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}
	callCode := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, &hostError{code: "host_unavailable", message: fmt.Sprintf("host callback %s returned no response, code=%d", method, int(callCode))}
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(rawResponse, &env); errUnmarshal != nil {
		return nil, &hostError{code: "host_protocol", message: fmt.Sprintf("decode host envelope %s: %v", method, errUnmarshal)}
	}
	if !env.OK {
		if env.Error != nil {
			return nil, &hostError{code: env.Error.Code, message: env.Error.Message, httpStatus: env.Error.HTTPStatus}
		}
		return nil, &hostError{code: "host_error", message: fmt.Sprintf("host callback %s failed", method)}
	}
	if callCode != 0 {
		return nil, &hostError{code: "host_code", message: fmt.Sprintf("host callback %s returned code=%d", method, int(callCode))}
	}
	return append(json.RawMessage(nil), env.Result...), nil
}

// logHost emits a log line through the host logging callback.
func logHost(level, message string, fields map[string]any) {
	_, _ = callHost("host.log", hostLogRequest{Level: level, Message: message, Fields: fields})
}

// ---------------------------------------------------------------------------
// Envelope helpers
// ---------------------------------------------------------------------------

func jsonUnmarshal(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}

func okEnvelope(v any) ([]byte, error) {
	raw, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}

func errorEnvelopeBytes(code, message string, httpStatus int) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message, HTTPStatus: httpStatus}})
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}
