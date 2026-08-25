package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
)

var ErrInvalidSupervisor = errors.New("invalid gateway worker supervisor configuration")

type ConnectionSource interface {
	ListConnectionsPage(context.Context, domain.TenantID, domain.ConnectionID, int) (postgres.ConnectionPage, error)
}

type ActorController interface {
	Start(context.Context, connectionactor.Key) error
	Ready(context.Context, connectionactor.Key) bool
	Quarantine(context.Context, connectionactor.Key) error
	Shutdown(context.Context) error
}

type LaneSource interface {
	ListQueuedDispatchLanes(context.Context, domain.TenantID, int) ([]messaging.LaneKey, error)
}

type paginatedLaneSource interface {
	ListQueuedDispatchLanesAfter(context.Context, domain.TenantID, messaging.LaneKey, int) ([]messaging.LaneKey, error)
}

type LaneDispatcher interface {
	DispatchLane(context.Context, messaging.LaneKey) (bool, error)
}

type OneWorker interface {
	RunOne(context.Context) (bool, error)
}

type ConnectionWorker interface {
	RunConnection(context.Context, connectionactor.Key) (bool, error)
}

type BatchWorker interface {
	RunBatch(context.Context) error
}

type BackfillQuarantine interface {
	QuarantineBackfillConnection(context.Context, domain.TenantID, domain.ConnectionID, string) error
}

type SupervisorConfig struct {
	Tenants             []domain.TenantID
	Connections         ConnectionSource
	Actors              ActorController
	Lanes               LaneSource
	Dispatcher          LaneDispatcher
	Media               map[domain.TenantID]OneWorker
	Backfill            map[domain.TenantID]ConnectionWorker
	BackfillQuarantine  BackfillQuarantine
	Webhooks            map[domain.TenantID]BatchWorker
	Kafka               map[domain.TenantID]BatchWorker
	PollInterval        time.Duration
	ReadyTimeout        time.Duration
	JoinTimeout         time.Duration
	LaneLimit           int
	DispatchConcurrency int
	FatalErrorThreshold int
	MediaLimit          int
	BackfillLimit       int
	ConnectionLimit     int
	OnError             func(error)
}

type Supervisor struct {
	tenants             []domain.TenantID
	connections         ConnectionSource
	actors              ActorController
	lanes               LaneSource
	dispatcher          LaneDispatcher
	media               map[domain.TenantID]OneWorker
	backfill            map[domain.TenantID]ConnectionWorker
	backfillQuarantine  BackfillQuarantine
	webhooks            map[domain.TenantID]BatchWorker
	kafka               map[domain.TenantID]BatchWorker
	pollInterval        time.Duration
	readyTimeout        time.Duration
	joinTimeout         time.Duration
	laneLimit           int
	dispatchConcurrency int
	fatalErrorThreshold int
	mediaLimit          int
	backfillLimit       int
	connectionLimit     int
	onError             func(error)
	fatal               chan error
	stateMu             sync.Mutex
	laneCursors         map[domain.TenantID]messaging.LaneKey
	actorCursors        map[domain.TenantID]domain.ConnectionID
	backfillCursors     map[domain.TenantID]domain.ConnectionID
	backfillFailures    map[connectionactor.Key]int
	backfillQuarantined map[connectionactor.Key]struct{}
	actorFailures       map[connectionactor.Key]int
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if config.PollInterval == 0 {
		config.PollInterval = time.Second
	}
	if config.ReadyTimeout == 0 {
		config.ReadyTimeout = time.Second
	}
	if config.JoinTimeout == 0 {
		config.JoinTimeout = 10 * time.Second
	}
	if config.LaneLimit == 0 {
		config.LaneLimit = 64
	}
	if config.MediaLimit == 0 {
		config.MediaLimit = 16
	}
	if config.BackfillLimit == 0 {
		config.BackfillLimit = 16
	}
	if config.ConnectionLimit == 0 {
		config.ConnectionLimit = 64
	}
	if config.DispatchConcurrency == 0 {
		config.DispatchConcurrency = 8
	}
	if config.FatalErrorThreshold == 0 {
		config.FatalErrorThreshold = 5
	}
	if len(config.Tenants) == 0 || len(config.Tenants) > 1024 || config.Connections == nil || config.Actors == nil || config.Lanes == nil || config.Dispatcher == nil ||
		config.PollInterval < 10*time.Millisecond || config.PollInterval > time.Minute ||
		config.ReadyTimeout < time.Millisecond || config.ReadyTimeout > 10*time.Second ||
		config.JoinTimeout < 10*time.Millisecond || config.JoinTimeout > time.Minute ||
		config.LaneLimit < 1 || config.LaneLimit > 256 || config.DispatchConcurrency < 1 || config.DispatchConcurrency > 64 ||
		config.FatalErrorThreshold < 1 || config.FatalErrorThreshold > 100 || config.MediaLimit < 1 || config.MediaLimit > 64 ||
		config.BackfillLimit < 1 || config.BackfillLimit > 64 || config.ConnectionLimit < 1 || config.ConnectionLimit > 256 {
		return nil, ErrInvalidSupervisor
	}
	trusted := make(map[domain.TenantID]struct{}, len(config.Tenants))
	for _, tenantID := range config.Tenants {
		if tenantID == "" {
			return nil, ErrInvalidSupervisor
		}
		if _, duplicate := trusted[tenantID]; duplicate {
			return nil, ErrInvalidSupervisor
		}
		trusted[tenantID] = struct{}{}
	}
	for _, workers := range []map[domain.TenantID]BatchWorker{config.Webhooks, config.Kafka} {
		for tenantID, worker := range workers {
			if _, ok := trusted[tenantID]; !ok || worker == nil {
				return nil, ErrInvalidSupervisor
			}
		}
	}
	for tenantID, worker := range config.Media {
		if _, ok := trusted[tenantID]; !ok || worker == nil {
			return nil, ErrInvalidSupervisor
		}
	}
	for tenantID, worker := range config.Backfill {
		if _, ok := trusted[tenantID]; !ok || worker == nil {
			return nil, ErrInvalidSupervisor
		}
	}
	if len(config.Backfill) > 0 && config.BackfillQuarantine == nil {
		return nil, ErrInvalidSupervisor
	}
	if config.OnError == nil {
		config.OnError = func(error) {}
	}
	return &Supervisor{
		tenants: append([]domain.TenantID(nil), config.Tenants...), connections: config.Connections,
		actors: config.Actors, lanes: config.Lanes, dispatcher: config.Dispatcher,
		media: config.Media, backfill: config.Backfill, backfillQuarantine: config.BackfillQuarantine, webhooks: config.Webhooks, kafka: config.Kafka,
		pollInterval: config.PollInterval, readyTimeout: config.ReadyTimeout, joinTimeout: config.JoinTimeout,
		laneLimit: config.LaneLimit, dispatchConcurrency: config.DispatchConcurrency, fatalErrorThreshold: config.FatalErrorThreshold,
		mediaLimit: config.MediaLimit, backfillLimit: config.BackfillLimit, connectionLimit: config.ConnectionLimit,
		onError: config.OnError, fatal: make(chan error, 1),
		laneCursors:         make(map[domain.TenantID]messaging.LaneKey, len(config.Tenants)),
		actorCursors:        make(map[domain.TenantID]domain.ConnectionID, len(config.Tenants)),
		backfillCursors:     make(map[domain.TenantID]domain.ConnectionID, len(config.Tenants)),
		backfillFailures:    make(map[connectionactor.Key]int),
		backfillQuarantined: make(map[connectionactor.Key]struct{}),
		actorFailures:       make(map[connectionactor.Key]int),
	}, nil
}

func (supervisor *Supervisor) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidSupervisor
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	launch := func(work func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			supervisor.runLoop(workerCtx, work)
		}()
	}
	launch(supervisor.discoverActors)
	for _, tenantID := range supervisor.tenants {
		tenantID := tenantID
		launch(func(loopCtx context.Context) error { return supervisor.dispatch(loopCtx, tenantID) })
		if worker := supervisor.media[tenantID]; worker != nil {
			launch(func(loopCtx context.Context) error { return supervisor.drainMedia(loopCtx, worker) })
		}
		if worker := supervisor.backfill[tenantID]; worker != nil {
			launch(func(loopCtx context.Context) error { return supervisor.runBackfill(loopCtx, tenantID, worker) })
		}
		if worker := supervisor.webhooks[tenantID]; worker != nil {
			launch(worker.RunBatch)
		}
		if worker := supervisor.kafka[tenantID]; worker != nil {
			launch(worker.RunBatch)
		}
	}
	var fatalErr error
	select {
	case <-ctx.Done():
	case fatalErr = <-supervisor.fatal:
	}
	cancelWorkers()
	workers.Wait()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), supervisor.joinTimeout)
	defer cancelShutdown()
	if err := supervisor.actors.Shutdown(shutdownCtx); err != nil {
		return errors.Join(fatalErr, err)
	}
	return fatalErr
}

func (supervisor *Supervisor) runLoop(ctx context.Context, work func(context.Context) error) {
	ticker := time.NewTicker(supervisor.pollInterval)
	defer ticker.Stop()
	consecutiveFailures := 0
	for {
		if err := work(ctx); err != nil && ctx.Err() == nil {
			consecutiveFailures++
			supervisor.onError(err)
			if consecutiveFailures >= supervisor.fatalErrorThreshold {
				select {
				case supervisor.fatal <- err:
				default:
				}
				return
			}
		} else {
			consecutiveFailures = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (supervisor *Supervisor) discoverActors(ctx context.Context) error {
	for _, tenantID := range supervisor.tenants {
		supervisor.stateMu.Lock()
		cursor := supervisor.actorCursors[tenantID]
		supervisor.stateMu.Unlock()
		page, err := supervisor.connections.ListConnectionsPage(ctx, tenantID, cursor, supervisor.connectionLimit)
		if err == nil && len(page.Records) == 0 && cursor != "" {
			cursor = ""
			page, err = supervisor.connections.ListConnectionsPage(ctx, tenantID, cursor, supervisor.connectionLimit)
		}
		if err != nil {
			return err
		}
		supervisor.stateMu.Lock()
		supervisor.actorCursors[tenantID] = page.NextCursor
		supervisor.stateMu.Unlock()
		for _, record := range page.Records {
			connection := record.Connection
			if connection.TenantID != tenantID || connection.ID == "" {
				return errors.New("connection source returned a cross-tenant connection")
			}
			if connection.State != domain.ConnectionStateConnected && connection.State != domain.ConnectionStateDegraded {
				continue
			}
			key := connectionactor.Key{TenantID: tenantID, ConnectionID: connection.ID}
			if err = supervisor.actors.Start(ctx, key); err != nil && !errors.Is(err, context.Canceled) {
				if connectionLocalActorError(err) {
					supervisor.onError(err)
					continue
				}
				if errors.Is(err, connectionactor.ErrSharedInfrastructure) {
					supervisor.onError(err)
					supervisor.recordActorInfrastructureFailure(key, err)
					continue
				}
				return err
			}
			supervisor.stateMu.Lock()
			delete(supervisor.actorFailures, key)
			supervisor.stateMu.Unlock()
		}
	}
	return nil
}

func (supervisor *Supervisor) recordActorInfrastructureFailure(key connectionactor.Key, failure error) {
	supervisor.stateMu.Lock()
	supervisor.actorFailures[key]++
	count := supervisor.actorFailures[key]
	supervisor.stateMu.Unlock()
	if count < supervisor.fatalErrorThreshold {
		return
	}
	select {
	case supervisor.fatal <- failure:
	default:
	}
}

func (supervisor *Supervisor) dispatch(ctx context.Context, tenantID domain.TenantID) error {
	var lanes []messaging.LaneKey
	var err error
	if paged, ok := supervisor.lanes.(paginatedLaneSource); ok {
		supervisor.stateMu.Lock()
		cursor := supervisor.laneCursors[tenantID]
		supervisor.stateMu.Unlock()
		lanes, err = paged.ListQueuedDispatchLanesAfter(ctx, tenantID, cursor, supervisor.laneLimit)
		if err == nil && len(lanes) == 0 && cursor.ConnectionID != "" {
			cursor = messaging.LaneKey{}
			lanes, err = paged.ListQueuedDispatchLanesAfter(ctx, tenantID, cursor, supervisor.laneLimit)
		}
		if len(lanes) > 0 {
			supervisor.stateMu.Lock()
			supervisor.laneCursors[tenantID] = lanes[len(lanes)-1]
			supervisor.stateMu.Unlock()
		}
	} else {
		lanes, err = supervisor.lanes.ListQueuedDispatchLanes(ctx, tenantID, supervisor.laneLimit)
	}
	if err != nil {
		return err
	}
	for _, lane := range lanes {
		if lane.TenantID != tenantID || lane.ConnectionID == "" || lane.ConversationID == "" {
			return errors.New("lane source returned a cross-tenant lane")
		}
	}
	if len(lanes) == 0 {
		return nil
	}
	workers := min(supervisor.dispatchConcurrency, len(lanes))
	queue := make(chan messaging.LaneKey, workers)
	errorsFound := make(chan error, len(lanes))
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for lane := range queue {
				if itemErr := supervisor.dispatchLane(ctx, lane); itemErr != nil && ctx.Err() == nil {
					errorsFound <- itemErr
				}
			}
		}()
	}
	for _, lane := range lanes {
		select {
		case queue <- lane:
		case <-ctx.Done():
			close(queue)
			wait.Wait()
			return ctx.Err()
		}
	}
	close(queue)
	wait.Wait()
	close(errorsFound)
	var joined error
	for itemErr := range errorsFound {
		joined = errors.Join(joined, itemErr)
	}
	return joined
}

func (supervisor *Supervisor) dispatchLane(ctx context.Context, lane messaging.LaneKey) error {
	key := connectionactor.Key{TenantID: lane.TenantID, ConnectionID: lane.ConnectionID}
	if err := supervisor.actors.Start(ctx, key); err != nil {
		if connectionLocalActorError(err) {
			supervisor.onError(err)
			return nil
		}
		return err
	}
	readyCtx, cancelReady := context.WithTimeout(ctx, supervisor.readyTimeout)
	ready := supervisor.actors.Ready(readyCtx, key)
	cancelReady()
	if !ready {
		return nil
	}
	_, err := supervisor.dispatcher.DispatchLane(ctx, lane)
	return err
}

func connectionLocalActorError(err error) bool {
	return errors.Is(err, connectionactor.ErrStaleGeneration) ||
		errors.Is(err, connectionactor.ErrProviderUnavailable) ||
		errors.Is(err, connectionactor.ErrProviderTransient) ||
		errors.Is(err, connectionactor.ErrProviderAuthorization) ||
		errors.Is(err, connectionactor.ErrProviderPermanentConfig) ||
		errors.Is(err, connectionactor.ErrProviderPermanentProtocol) ||
		errors.Is(err, connectionactor.ErrProviderJoinTimeout)
}

func (supervisor *Supervisor) drainMedia(ctx context.Context, worker OneWorker) error {
	for count := 0; count < supervisor.mediaLimit; count++ {
		processed, err := worker.RunOne(ctx)
		if err != nil || !processed {
			return err
		}
	}
	return nil
}

func (supervisor *Supervisor) runBackfill(ctx context.Context, tenantID domain.TenantID, worker ConnectionWorker) error {
	supervisor.stateMu.Lock()
	cursor := supervisor.backfillCursors[tenantID]
	supervisor.stateMu.Unlock()
	page, err := supervisor.connections.ListConnectionsPage(ctx, tenantID, cursor, supervisor.backfillLimit)
	if err == nil && len(page.Records) == 0 && cursor != "" {
		cursor = ""
		page, err = supervisor.connections.ListConnectionsPage(ctx, tenantID, cursor, supervisor.backfillLimit)
	}
	if err != nil {
		return err
	}
	supervisor.stateMu.Lock()
	supervisor.backfillCursors[tenantID] = page.NextCursor
	supervisor.stateMu.Unlock()
	eligible := make([]connectionactor.Key, 0, len(page.Records))
	for _, record := range page.Records {
		connection := record.Connection
		if connection.TenantID != tenantID || connection.ID == "" {
			return errors.New("connection source returned a cross-tenant connection")
		}
		if connection.State == domain.ConnectionStateConnected || connection.State == domain.ConnectionStateDegraded {
			eligible = append(eligible, connectionactor.Key{TenantID: tenantID, ConnectionID: connection.ID})
		}
	}
	if len(eligible) == 0 {
		return nil
	}
	var failures error
	for _, key := range eligible {
		supervisor.stateMu.Lock()
		_, quarantined := supervisor.backfillQuarantined[key]
		supervisor.stateMu.Unlock()
		if quarantined {
			continue
		}
		if startErr := supervisor.actors.Start(ctx, key); startErr != nil {
			if ctx.Err() == nil {
				supervisor.onError(startErr)
				if quarantineErr := supervisor.recordBackfillResult(ctx, key, startErr); quarantineErr != nil {
					failures = errors.Join(failures, quarantineErr)
				}
			}
			continue
		}
		readyCtx, cancelReady := context.WithTimeout(ctx, supervisor.readyTimeout)
		ready := supervisor.actors.Ready(readyCtx, key)
		cancelReady()
		if !ready {
			continue
		}
		if _, itemErr := worker.RunConnection(ctx, key); itemErr != nil && ctx.Err() == nil {
			supervisor.onError(itemErr)
			if quarantineErr := supervisor.recordBackfillResult(ctx, key, itemErr); quarantineErr != nil {
				failures = errors.Join(failures, quarantineErr)
			}
		} else if itemErr == nil {
			_ = supervisor.recordBackfillResult(ctx, key, nil)
		}
	}
	return failures
}

func (supervisor *Supervisor) recordBackfillResult(ctx context.Context, key connectionactor.Key, itemErr error) error {
	supervisor.stateMu.Lock()
	if itemErr == nil {
		delete(supervisor.backfillFailures, key)
		supervisor.stateMu.Unlock()
		return nil
	}
	if _, quarantined := supervisor.backfillQuarantined[key]; quarantined {
		supervisor.stateMu.Unlock()
		return nil
	}
	if errors.Is(itemErr, connectionactor.ErrStaleGeneration) {
		delete(supervisor.backfillFailures, key)
		supervisor.stateMu.Unlock()
		return nil
	}
	supervisor.backfillFailures[key]++
	count := supervisor.backfillFailures[key]
	if errors.Is(itemErr, messaging.ErrBackfillPoisoned) || errors.Is(itemErr, connectionactor.ErrProviderPermanentProtocol) {
		count = supervisor.fatalErrorThreshold
		supervisor.backfillFailures[key] = count
	}
	supervisor.stateMu.Unlock()
	if count < supervisor.fatalErrorThreshold {
		return nil
	}
	if errors.Is(itemErr, connectionactor.ErrSharedInfrastructure) {
		select {
		case supervisor.fatal <- itemErr:
		default:
		}
		return nil
	}
	if err := supervisor.backfillQuarantine.QuarantineBackfillConnection(ctx, key.TenantID, key.ConnectionID, "provider-protocol"); err != nil {
		return err
	}
	quarantineCtx, cancelQuarantine := context.WithTimeout(ctx, supervisor.joinTimeout)
	actorErr := supervisor.actors.Quarantine(quarantineCtx, key)
	cancelQuarantine()
	supervisor.stateMu.Lock()
	supervisor.backfillQuarantined[key] = struct{}{}
	delete(supervisor.backfillFailures, key)
	supervisor.stateMu.Unlock()
	return actorErr
}
