package ops

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueueDepths struct {
	Messages int64
	Media    int64
	Webhooks int64
	Kafka    int64
}

type QueueSource interface {
	QueueDepths(context.Context) (QueueDepths, error)
}

type QueueSourceFunc func(context.Context) (QueueDepths, error)

func (source QueueSourceFunc) QueueDepths(ctx context.Context) (QueueDepths, error) {
	return source(ctx)
}

type Registry struct {
	db     *sql.DB
	queues QueueSource
	mu     sync.Mutex
	values map[string]uint64
}

func NewRegistry(db *sql.DB, queues QueueSource) *Registry {
	return &Registry{db: db, queues: queues, values: make(map[string]uint64)}
}

func (registry *Registry) increment(key string) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	registry.values[key]++
	registry.mu.Unlock()
}

func (registry *Registry) ActorState(state string) {
	registry.increment("actor_state:" + classifyActorState(state))
}

func (registry *Registry) LeaseAcquired() { registry.increment("actor_lease:acquired") }
func (registry *Registry) LeaseLost()     { registry.increment("actor_lease:lost") }

func (registry *Registry) Reconnect(reason string) {
	registry.increment("actor_reconnect:" + classifyActorReason(reason))
}

func (registry *Registry) ReadinessCheck(dependency Dependency, ready bool, class FailureClass) {
	if !validDependency(dependency) {
		dependency = DependencyGateway
	}
	result := "failed"
	if ready {
		result, class = "ready", FailureNone
	} else {
		class = classifyFailure(func(error) FailureClass { return class }, nil)
	}
	registry.increment("readiness:" + string(dependency) + ":" + result + ":" + string(class))
}

func (registry *Registry) Backoff(reason string, duration time.Duration) {
	registry.increment("actor_backoff:" + classifyActorReason(reason))
	if registry == nil || duration <= 0 {
		return
	}
	registry.mu.Lock()
	registry.values["actor_backoff_seconds_total"] += uint64(duration / time.Second)
	registry.mu.Unlock()
}

func classifyActorState(value string) string {
	switch value {
	case "acquiring", "connecting", "ready", "backoff", "stopped", "lease-lost":
		return value
	default:
		return "unknown"
	}
}

func classifyActorReason(value string) string {
	switch value {
	case "none", "transient-network", "provider-auth", "provider-config", "provider-protocol", "shared-infrastructure", "lease-lost", "session-conflict", "shutdown":
		return value
	default:
		return "unknown"
	}
}

func (registry *Registry) WrapHTTP(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		capture := &statusCapture{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(capture, request)
		registry.increment("http:" + classifyMethod(request.Method) + ":" + classifyStatus(capture.status))
	})
}

type statusCapture struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (capture *statusCapture) Unwrap() http.ResponseWriter { return capture.ResponseWriter }

func (capture *statusCapture) WriteHeader(status int) {
	if capture.wroteHeader {
		return
	}
	capture.wroteHeader = true
	capture.status = status
	capture.ResponseWriter.WriteHeader(status)
}

func (capture *statusCapture) Write(contents []byte) (int, error) {
	if !capture.wroteHeader {
		capture.WriteHeader(http.StatusOK)
	}
	return capture.ResponseWriter.Write(contents)
}

func classifyMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return method
	default:
		return "OTHER"
	}
}

func classifyStatus(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}

func (registry *Registry) Prometheus(ctx context.Context) string {
	if registry == nil {
		return ""
	}
	registry.mu.Lock()
	values := make(map[string]uint64, len(registry.values))
	for key, value := range registry.values {
		values[key] = value
	}
	registry.mu.Unlock()

	var lines []string
	for key, value := range values {
		parts := strings.Split(key, ":")
		switch parts[0] {
		case "http":
			lines = append(lines, fmt.Sprintf("sirenaix_http_requests_total{method=%q,status=%q} %d", parts[1], parts[2], value))
		case "actor_state":
			lines = append(lines, fmt.Sprintf("sirenaix_actor_state_transitions_total{state=%q} %d", parts[1], value))
		case "actor_lease":
			lines = append(lines, fmt.Sprintf("sirenaix_actor_leases_total{result=%q} %d", parts[1], value))
		case "actor_reconnect":
			lines = append(lines, fmt.Sprintf("sirenaix_actor_reconnects_total{reason=%q} %d", parts[1], value))
		case "actor_backoff":
			lines = append(lines, fmt.Sprintf("sirenaix_actor_backoffs_total{reason=%q} %d", parts[1], value))
		case "actor_backoff_seconds_total":
			lines = append(lines, fmt.Sprintf("sirenaix_actor_backoff_seconds_total %d", value))
		case "readiness":
			lines = append(lines, fmt.Sprintf("sirenaix_readiness_checks_total{dependency=%q,result=%q,class=%q} %d", parts[1], parts[2], parts[3], value))
		}
	}
	lines = append(lines, fmt.Sprintf("sirenaix_process_goroutines %d", runtime.NumGoroutine()))
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	lines = append(lines, fmt.Sprintf("sirenaix_process_go_memory_bytes{kind=%q} %d", "sys", memory.Sys))
	if registry.db != nil {
		stats := registry.db.Stats()
		lines = append(lines,
			fmt.Sprintf("sirenaix_database_connections{state=%q} %d", "open", stats.OpenConnections),
			fmt.Sprintf("sirenaix_database_connections{state=%q} %d", "in_use", stats.InUse),
			fmt.Sprintf("sirenaix_database_connections{state=%q} %d", "idle", stats.Idle),
			fmt.Sprintf("sirenaix_database_wait_total %d", stats.WaitCount),
		)
	}
	if registry.queues != nil {
		queueCtx, cancel := context.WithTimeout(ctx, time.Second)
		depths, err := registry.queues.QueueDepths(queueCtx)
		cancel()
		success := 1
		if err != nil {
			success = 0
			depths = QueueDepths{}
		}
		lines = append(lines,
			fmt.Sprintf("sirenaix_queue_collection_success %d", success),
			fmt.Sprintf("sirenaix_queue_depth{queue=%q} %d", "messages", depths.Messages),
			fmt.Sprintf("sirenaix_queue_depth{queue=%q} %d", "media", depths.Media),
			fmt.Sprintf("sirenaix_queue_depth{queue=%q} %d", "webhooks", depths.Webhooks),
			fmt.Sprintf("sirenaix_queue_depth{queue=%q} %d", "kafka", depths.Kafka),
		)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}
