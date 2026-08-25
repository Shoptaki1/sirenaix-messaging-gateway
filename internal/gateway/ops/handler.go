package ops

import (
	"context"
	"errors"
	"net/http"
	"time"
)

var ErrInvalidConfig = errors.New("invalid operations listener configuration")

type Dependency string

const (
	DependencyDatabase    Dependency = "database"
	DependencySchema      Dependency = "schema"
	DependencyGateway     Dependency = "gateway"
	DependencyKafka       Dependency = "kafka"
	DependencyObjectStore Dependency = "object_store"
)

type FailureClass string

const (
	FailureNone                      FailureClass = "none"
	FailureTimeout                   FailureClass = "timeout"
	FailureUnavailable               FailureClass = "unavailable"
	FailureSchema                    FailureClass = "schema"
	FailureStartup                   FailureClass = "startup"
	FailureMissing                   FailureClass = "missing"
	FailureAuthorization             FailureClass = "authorization"
	FailureAuthorizationUnverifiable FailureClass = "authorization_unverifiable"
	FailureUnknown                   FailureClass = "unknown"
)

type Check interface {
	Check(context.Context) error
}

type NamedCheck struct {
	Name            Dependency
	Check           Check
	ClassifyFailure func(error) FailureClass
}

type Config struct {
	Registry           *Registry
	Checks             []NamedCheck
	CheckTimeout       time.Duration
	OnReadinessFailure func(Dependency, FailureClass)
}

func NewHandler(config Config) (http.Handler, error) {
	if config.Registry == nil {
		return nil, ErrInvalidConfig
	}
	if config.CheckTimeout == 0 {
		config.CheckTimeout = 2 * time.Second
	}
	if config.CheckTimeout < 10*time.Millisecond || config.CheckTimeout > 10*time.Second {
		return nil, ErrInvalidConfig
	}
	dependencies := make(map[Dependency]struct{}, len(config.Checks))
	for _, check := range config.Checks {
		if !validDependency(check.Name) || check.Check == nil {
			return nil, ErrInvalidConfig
		}
		if _, duplicate := dependencies[check.Name]; duplicate {
			return nil, ErrInvalidConfig
		}
		dependencies[check.Name] = struct{}{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), config.CheckTimeout)
		defer cancel()
		for _, named := range config.Checks {
			if err := named.Check.Check(ctx); err != nil {
				class := classifyFailure(named.ClassifyFailure, err)
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					class = FailureTimeout
				}
				config.Registry.ReadinessCheck(named.Name, false, class)
				if config.OnReadinessFailure != nil {
					config.OnReadinessFailure(named.Name, class)
				}
				writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
				writer.WriteHeader(http.StatusServiceUnavailable)
				_, _ = writer.Write([]byte("unavailable\n"))
				return
			}
			config.Registry.ReadinessCheck(named.Name, true, FailureNone)
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(config.Registry.Prometheus(request.Context())))
	})
	return mux, nil
}

func validDependency(dependency Dependency) bool {
	switch dependency {
	case DependencyDatabase, DependencySchema, DependencyGateway, DependencyKafka, DependencyObjectStore:
		return true
	default:
		return false
	}
}

func classifyFailure(classifier func(error) FailureClass, err error) FailureClass {
	if classifier == nil {
		return FailureUnavailable
	}
	class := classifier(err)
	switch class {
	case FailureTimeout, FailureUnavailable, FailureSchema, FailureStartup, FailureMissing, FailureAuthorization, FailureAuthorizationUnverifiable:
		return class
	default:
		return FailureUnknown
	}
}

type CheckFunc func(context.Context) error

func (check CheckFunc) Check(ctx context.Context) error { return check(ctx) }
