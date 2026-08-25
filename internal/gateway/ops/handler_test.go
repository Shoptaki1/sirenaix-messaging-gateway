package ops

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type readinessCheckFunc func(context.Context) error

func (check readinessCheckFunc) Check(ctx context.Context) error { return check(ctx) }

func TestHealthEndpointsSeparateLivenessFromMandatoryReadiness(t *testing.T) {
	ready := false
	handler, err := NewHandler(Config{
		Registry: NewRegistry(nil, nil),
		Checks: []NamedCheck{{Name: "gateway", Check: readinessCheckFunc(func(context.Context) error {
			if !ready {
				return errors.New("not started")
			}
			return nil
		})}},
		CheckTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if live.Code != http.StatusOK || strings.TrimSpace(live.Body.String()) != "ok" {
		t.Fatalf("live response = %d %q", live.Code, live.Body.String())
	}
	notReady := httptest.NewRecorder()
	handler.ServeHTTP(notReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if notReady.Code != http.StatusServiceUnavailable || strings.Contains(notReady.Body.String(), "not started") {
		t.Fatalf("unready response leaked detail or wrong status = %d %q", notReady.Code, notReady.Body.String())
	}
	ready = true
	readyResponse := httptest.NewRecorder()
	handler.ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyResponse.Code != http.StatusOK {
		t.Fatalf("ready response = %d %q", readyResponse.Code, readyResponse.Body.String())
	}
}

func TestMetricsUseOnlyBoundedLabelsAndNeverExposeTenantConnectionOrRequestPath(t *testing.T) {
	registry := NewRegistry(&sql.DB{}, QueueSourceFunc(func(context.Context) (QueueDepths, error) {
		return QueueDepths{Messages: 2, Media: 3, Webhooks: 4, Kafka: 5}, nil
	}))
	wrapped := registry.WrapHTTP(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
	}))
	wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/tenants/secret-tenant/connections/secret-phone/messages", nil))
	registry.ActorState("ready")
	registry.ActorState("secret-connection")
	registry.Reconnect("provider-auth")
	registry.Reconnect("secret-tenant")
	registry.LeaseAcquired()

	output := registry.Prometheus(context.Background())
	for _, secret := range []string{"secret-tenant", "secret-phone", "secret-connection", "/v1/tenants"} {
		if strings.Contains(output, secret) {
			t.Fatalf("metrics leaked %q:\n%s", secret, output)
		}
	}
	for _, expected := range []string{
		`sirenaix_http_requests_total{method="POST",status="2xx"} 1`,
		`sirenaix_actor_state_transitions_total{state="ready"} 1`,
		`sirenaix_actor_state_transitions_total{state="unknown"} 1`,
		`sirenaix_actor_reconnects_total{reason="provider-auth"} 1`,
		`sirenaix_actor_reconnects_total{reason="unknown"} 1`,
		`sirenaix_queue_depth{queue="messages"} 2`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, output)
		}
	}
}

func TestHTTPMetricsRecordTheFirstCommittedStatusOnly(t *testing.T) {
	registry := NewRegistry(nil, nil)
	wrapped := registry.WrapHTTP(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("already committed"))
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	wrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/anything", nil))
	output := registry.Prometheus(context.Background())
	if !strings.Contains(output, `sirenaix_http_requests_total{method="GET",status="2xx"} 1`) || strings.Contains(output, `status="5xx"`) {
		t.Fatalf("HTTP status metrics =\n%s", output)
	}
}

func TestReadinessTimeoutIsBoundedAndResponseIsRedacted(t *testing.T) {
	handler, err := NewHandler(Config{
		Registry: NewRegistry(nil, nil), CheckTimeout: 20 * time.Millisecond,
		Checks: []NamedCheck{{Name: "database", Check: readinessCheckFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})}},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if time.Since(started) > 250*time.Millisecond || response.Code != http.StatusServiceUnavailable {
		t.Fatalf("bounded readiness = %v, status %d", time.Since(started), response.Code)
	}
	if strings.Contains(strings.ToLower(response.Body.String()), "deadline") {
		t.Fatalf("readiness leaked internal error: %q", response.Body.String())
	}
}

func TestReadinessDiagnosticsUseOnlyFixedDependencyAndFailureClasses(t *testing.T) {
	secret := "postgres://operator:secret@database.invalid/private"
	registry := NewRegistry(nil, nil)
	var reportedDependency Dependency
	var reportedClass FailureClass
	handler, err := NewHandler(Config{
		Registry: registry,
		Checks: []NamedCheck{{
			Name: DependencyKafka,
			Check: readinessCheckFunc(func(context.Context) error {
				return errors.New(secret)
			}),
			ClassifyFailure: func(error) FailureClass { return FailureClass(secret) },
		}},
		OnReadinessFailure: func(dependency Dependency, class FailureClass) {
			reportedDependency, reportedClass = dependency, class
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	metrics := registry.Prometheus(context.Background())
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), secret) || strings.Contains(metrics, secret) {
		t.Fatalf("unsafe readiness diagnostics: status=%d body=%q metrics=%q", response.Code, response.Body.String(), metrics)
	}
	if reportedDependency != DependencyKafka || reportedClass != FailureUnknown {
		t.Fatalf("reported readiness failure = (%q, %q)", reportedDependency, reportedClass)
	}
	if !strings.Contains(metrics, `sirenaix_readiness_checks_total{dependency="kafka",result="failed",class="unknown"} 1`) {
		t.Fatalf("missing bounded readiness metric:\n%s", metrics)
	}
}

func TestReadinessReportsAuthorizationUnverifiableAsABoundedFailureClass(t *testing.T) {
	registry := NewRegistry(nil, nil)
	var reported FailureClass
	handler, err := NewHandler(Config{
		Registry: registry,
		Checks: []NamedCheck{{
			Name: DependencyKafka,
			Check: readinessCheckFunc(func(context.Context) error {
				return errors.New("broker did not advertise authorized operations")
			}),
			ClassifyFailure: func(error) FailureClass { return FailureClass("authorization_unverifiable") },
		}},
		OnReadinessFailure: func(_ Dependency, class FailureClass) { reported = class },
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	metrics := registry.Prometheus(context.Background())
	if reported != FailureClass("authorization_unverifiable") ||
		!strings.Contains(metrics, `sirenaix_readiness_checks_total{dependency="kafka",result="failed",class="authorization_unverifiable"} 1`) {
		t.Fatalf("unverifiable authorization diagnostic = %q\n%s", reported, metrics)
	}
}

func TestReadinessRejectsUnknownOrDuplicateDependencyLabels(t *testing.T) {
	check := readinessCheckFunc(func(context.Context) error { return nil })
	for _, checks := range [][]NamedCheck{
		{{Name: Dependency("secret-tenant"), Check: check}},
		{{Name: DependencyDatabase, Check: check}, {Name: DependencyDatabase, Check: check}},
	} {
		if _, err := NewHandler(Config{Registry: NewRegistry(nil, nil), Checks: checks}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("NewHandler(%+v) error = %v, want ErrInvalidConfig", checks, err)
		}
	}
}
