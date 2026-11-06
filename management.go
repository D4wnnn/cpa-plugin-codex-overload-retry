package main

import (
	"bytes"
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"
)

const resourceStatusPath = "/status"

// managementRegistration mirrors pluginapi.ManagementRegistrationResponse
// (resource route fields marshal with Go names).
type managementRegistrationResponse struct {
	Resources []managementResourceRoute `json:"resources,omitempty"`
}

type managementResourceRoute struct {
	Path        string `json:"Path"`
	Menu        string `json:"Menu"`
	Description string `json:"Description"`
}

// managementRequest mirrors pluginhost's rpc management request
// (pluginapi.ManagementRequest fields marshal with Go names).
type managementRequest struct {
	Method string      `json:"Method"`
	Path   string      `json:"Path"`
	Body   []byte      `json:"Body"`
	Query  urlValues   `json:"Query"`
}

type urlValues map[string][]string

// managementResponse mirrors pluginapi.ManagementResponse (Go names).
type managementResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers,omitempty"`
	Body       []byte      `json:"Body"`
}

func registerManagement() ([]byte, error) {
	return okEnvelope(managementRegistrationResponse{
		Resources: []managementResourceRoute{{
			Path:        resourceStatusPath,
			Menu:        "Codex Overload Fix",
			Description: "Retry statistics for absorbed codex capacity errors.",
		}},
	})
}

func handleManagement(raw []byte) ([]byte, error) {
	var req managementRequest
	if len(raw) > 0 {
		if err := jsonUnmarshal(raw, &req); err != nil {
			return nil, err
		}
	}
	// Only one resource exists; serve the status page for any matching path.
	return okEnvelope(managementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       renderStatusPage(),
	})
}

func renderStatusPage() []byte {
	snap := stats.snapshot()
	cfg := loadConfig()

	var out bytes.Buffer
	out.WriteString("<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\">")
	out.WriteString("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">")
	out.WriteString("<meta http-equiv=\"refresh\" content=\"10\">")
	out.WriteString("<title>cpa-plugin-codex-overload-retry</title><style>")
	out.WriteString(pageCSS)
	out.WriteString("</style></head><body><main>")

	out.WriteString("<h1>cpa-plugin-codex-overload-retry <small>v" + html.EscapeString(pluginVersion) + "</small></h1>")
	out.WriteString("<p class=\"sub\">Absorbs codex \u201cSelected model is at capacity\u201d overload errors and retries before any output reaches the client. Auto-refreshes every 10s.</p>")

	// Uptime row.
	out.WriteString("<div class=\"cards\">")
	writeCard(&out, "运行时长", snap.Uptime.Round(time.Second).String(), "")
	writeCard(&out, "包装请求总数", fmt.Sprintf("%d", snap.WrappedTotal),
		fmt.Sprintf("stream %d · sync %d", snap.WrappedStream, snap.WrappedSync))
	writeCard(&out, "吸收的重试次数", fmt.Sprintf("%d", snap.Retries),
		"平均 "+fmt.Sprintf("%.1f", retryRatio(&snap))+" 次/请求")
	writeCard(&out, "重试耗尽", fmt.Sprintf("%d", snap.Exhausted), "最终把错误交还客户端")
	out.WriteString("</div>")

	// Config summary.
	out.WriteString("<h2>当前配置</h2><dl class=\"config\">")
	writeDef(&out, "source_formats", strings.Join(cfg.SourceFormats, ", "))
	writeDef(&out, "require_codex_provider", fmt.Sprintf("%v", cfg.RequireCodexProvider))
	writeDef(&out, "max_attempts", fmt.Sprintf("%d", cfg.MaxAttempts))
	writeDef(&out, "total_budget_seconds", fmt.Sprintf("%.0f", cfg.TotalBudgetSeconds))
	writeDef(&out, "commit_deadline_seconds", fmt.Sprintf("%.0f", cfg.CommitDeadlineSeconds))
	writeDef(&out, "backoff", fmt.Sprintf("%.1fs × %.1f, cap %.0fs",
		cfg.BackoffBaseSeconds, cfg.BackoffFactor, cfg.BackoffMaxSeconds))
	writeDef(&out, "max_retry_wait_seconds", fmt.Sprintf("%.0f", cfg.MaxRetryWaitSeconds))
	writeDef(&out, "retry_http_status", fmt.Sprintf("%v", cfg.RetryHTTPStatus))
	writeDef(&out, "prelude_limit_bytes", fmt.Sprintf("%d", cfg.PreludeLimitBytes))
	writeDef(&out, "log_retries / log_routes", fmt.Sprintf("%v / %v", cfg.LogRetries, cfg.LogRoutes))
	out.WriteString("</dl>")

	// Per-model table.
	out.WriteString("<h2>按模型统计</h2>")
	if len(snap.Models) == 0 {
		out.WriteString("<p class=\"empty\">尚无流量。</p>")
	} else {
		out.WriteString("<table><thead><tr><th>model</th><th>包装</th><th>重试</th><th>耗尽</th><th>致命</th></tr></thead><tbody>")
		for _, name := range snap.sortedModelNames() {
			m := snap.Models[name]
			fmt.Fprintf(&out, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td></tr>",
				html.EscapeString(name), m.Wrapped, m.Retries, m.Exhausted, m.Fatal)
		}
		out.WriteString("</tbody></table>")
	}

	// Recent retries.
	out.WriteString("<h2>最近的重试事件</h2>")
	if len(snap.Recent) == 0 {
		out.WriteString("<p class=\"empty\">还没有触发过重试——说明上游没有遇到过载，或尚未有流量。</p>")
	} else {
		out.WriteString("<table><thead><tr><th>时间</th><th>model</th><th>attempt</th><th>原因</th><th>等待</th></tr></thead><tbody>")
		for i := len(snap.Recent) - 1; i >= 0; i-- {
			ev := snap.Recent[i]
			fmt.Fprintf(&out, "<tr><td>%s</td><td>%s</td><td>%d</td><td class=\"reason\">%s</td><td>%s</td></tr>",
				ev.Time.Format("15:04:05"),
				html.EscapeString(ev.Model),
				ev.Attempt,
				html.EscapeString(ev.Reason),
				html.EscapeString(ev.Wait))
		}
		out.WriteString("</tbody></table>")
	}

	out.WriteString("<p class=\"foot\">数据保存在插件进程内，重启后清零。本页面不包含任何密钥或凭证信息。</p>")
	out.WriteString("</main></body></html>")
	return out.Bytes()
}

func retryRatio(snap *statsSnapshot) float64 {
	if snap.WrappedTotal == 0 {
		return 0
	}
	return float64(snap.Retries) / float64(snap.WrappedTotal)
}

func writeCard(out *bytes.Buffer, label, value, note string) {
	fmt.Fprintf(out, "<div class=\"card\"><div class=\"label\">%s</div><div class=\"value\">%s</div>", html.EscapeString(label), html.EscapeString(value))
	if note != "" {
		fmt.Fprintf(out, "<div class=\"note\">%s</div>", html.EscapeString(note))
	}
	out.WriteString("</div>")
}

func writeDef(out *bytes.Buffer, key, value string) {
	fmt.Fprintf(out, "<dt>%s</dt><dd><code>%s</code></dd>", html.EscapeString(key), html.EscapeString(value))
}

const pageCSS = `
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:#f5f6f8;color:#1f2933;margin:0;line-height:1.5}
main{max-width:920px;margin:2rem auto;padding:0 1rem}
h1{font-size:1.5rem;margin-bottom:.2rem}
h1 small{color:#6b7280;font-weight:400;font-size:1rem}
h2{font-size:1.05rem;margin:1.8rem 0 .6rem;color:#374151}
.sub{color:#6b7280;margin-top:0}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.8rem;margin:1.2rem 0}
.card{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:.8rem 1rem}
.card .label{color:#6b7280;font-size:.8rem;text-transform:uppercase;letter-spacing:.04em}
.card .value{font-size:1.6rem;font-weight:650;margin:.1rem 0}
.card .note{color:#9ca3af;font-size:.75rem}
dl.config{display:grid;grid-template-columns:max-content 1fr;gap:.3rem 1rem;background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:1rem}
dt{font-weight:600;color:#374151}
dd{margin:0}
table{border-collapse:collapse;width:100%;background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;font-size:.88rem}
th,td{padding:.5rem .8rem;text-align:left;border-bottom:1px solid #eef0f3}
th{background:#f9fafb;color:#4b5563;font-size:.78rem;text-transform:uppercase;letter-spacing:.03em}
td.reason{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.78rem;max-width:420px;word-break:break-all}
.empty{color:#6b7280;background:#fff;border:1px dashed #d1d5db;border-radius:10px;padding:1rem;text-align:center}
.foot{color:#9ca3af;font-size:.75rem;margin-top:1.5rem}
code{background:#f3f4f6;border-radius:4px;padding:.1rem .3rem;font-size:.85em}
`
