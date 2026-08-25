package libgm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"go.mau.fi/util/pblite"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

const defaultPingTimeout = 1 * time.Minute
const shortPingTimeout = 10 * time.Second
const minPingInterval = 30 * time.Second
const maxRepingTickerTime = 64 * time.Minute
const maxLongPollEnvelopeBytes = 4 << 20

var pingIDCounter atomic.Uint64

// Goals of the ditto pinger:
//   - By default, send pings to the phone every minute
//   - If an outgoing request doesn't respond quickly, send a ping immediately
//   - If a ping caused by a request timeout doesn't respond quickly, send PhoneNotResponding
//     (the user is probably actively trying to use the bridge)
//   - If the first ping doesn't respond, send PhoneNotResponding
//     (to avoid the bridge being stuck in the CONNECTING state)
//   - If a ping doesn't respond, send new pings on increasing intervals
//     (starting from 1 minute up to 1 hour) until it responds
//   - If a normal ping doesn't respond, send PhoneNotResponding after 3 failed pings
//     (so after ~8 minutes in total, not faster to avoid unnecessarily spamming the user)
//   - If a request timeout happens during backoff pings, send PhoneNotResponding immediately
//   - If a ping responds and PhoneNotResponding was sent, send PhoneRespondingAgain
type dittoPinger struct {
	client *Client

	firstPingDone     bool
	pingHandlingLock  sync.RWMutex
	oldestPingTime    time.Time
	lastPingTime      time.Time
	pingFails         int
	notRespondingSent bool
	pingInterval      time.Duration
	alertTimeoutCount int

	stop     <-chan struct{}
	ctx      context.Context
	log      *zerolog.Logger
	workerMu sync.Mutex
	workers  sync.WaitGroup
	stopping bool
}

type resetter struct {
	C chan struct{}
	d atomic.Bool
}

func newResetter() *resetter {
	return &resetter{
		C: make(chan struct{}),
	}
}

func (r *resetter) Done() {
	if r.d.CompareAndSwap(false, true) {
		close(r.C)
	}
}

func (dp *dittoPinger) OnRespond(pingID uint64, dur time.Duration, reset *resetter) {
	dp.pingHandlingLock.Lock()
	defer dp.pingHandlingLock.Unlock()
	logEvt := dp.log.Debug().Uint64("ping_id", pingID).Dur("duration", dur)
	if dp.notRespondingSent {
		logEvt.Msg("Ditto ping successful (phone is back online)")
		dp.client.triggerEvent(&events.PhoneRespondingAgain{})
	} else if dp.pingFails > 0 {
		logEvt.Msg("Ditto ping successful (stopped failing)")
		// TODO separate event?
		dp.client.triggerEvent(&events.PhoneRespondingAgain{})
	} else {
		logEvt.Msg("Ditto ping successful")
	}
	dp.oldestPingTime = time.Time{}
	dp.notRespondingSent = false
	dp.pingFails = 0
	dp.firstPingDone = true
	reset.Done()
}

func (dp *dittoPinger) OnTimeout(pingID uint64, sendNotResponding bool) {
	dp.pingHandlingLock.Lock()
	defer dp.pingHandlingLock.Unlock()
	dp.log.Warn().Uint64("ping_id", pingID).Msg("Ditto ping is taking long, phone may be offline")
	if (!dp.firstPingDone || sendNotResponding) && !dp.notRespondingSent {
		dp.client.triggerEvent(&events.PhoneNotResponding{})
		dp.notRespondingSent = true
	}
}

func (dp *dittoPinger) WaitForResponse(pingID uint64, start time.Time, timeout time.Duration, timeoutCount int, pingChan <-chan *IncomingRPCMessage, reset *resetter) {
	var timerChan <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerChan = timer.C
	}
	select {
	case <-pingChan:
		dp.OnRespond(pingID, time.Since(start), reset)
		if timer != nil && !timer.Stop() {
			<-timer.C
		}
	case <-timerChan:
		dp.OnTimeout(pingID, timeout == shortPingTimeout || timeoutCount >= dp.alertTimeoutCount)
		repingTickerTime := 1 * time.Minute
		var repingTicker *time.Ticker
		var repingTickerChan <-chan time.Time
		if timeoutCount == 0 {
			repingTicker = time.NewTicker(repingTickerTime)
			defer repingTicker.Stop()
			repingTickerChan = repingTicker.C
		}
		for {
			timeoutCount++
			select {
			case <-pingChan:
				dp.OnRespond(pingID, time.Since(start), reset)
				return
			case <-repingTickerChan:
				if repingTickerTime < maxRepingTickerTime {
					repingTickerTime *= 2
					repingTicker.Reset(repingTickerTime)
				}
				subPingID := pingIDCounter.Add(1)
				dp.log.Debug().
					Uint64("parent_ping_id", pingID).
					Uint64("ping_id", subPingID).
					Str("next_reping", repingTickerTime.String()).
					Msg("Sending new ping")
				dp.Ping(subPingID, defaultPingTimeout, timeoutCount, reset)
			case <-dp.client.pingShortCircuit:
				dp.pingHandlingLock.Lock()
				dp.log.Debug().Uint64("ping_id", pingID).
					Msg("Ditto ping wait short-circuited during ping backoff, sending PhoneNotResponding immediately")
				if !dp.notRespondingSent {
					dp.client.triggerEvent(&events.PhoneNotResponding{})
					dp.notRespondingSent = true
				}
				dp.pingHandlingLock.Unlock()
			case <-dp.stop:
				return
			case <-reset.C:
				dp.log.Debug().
					Uint64("ping_id", pingID).
					Msg("Another ping was successful, giving up on this one")
				return
			}
		}
	case <-reset.C:
		dp.log.Debug().
			Uint64("ping_id", pingID).
			Msg("Another ping was successful, giving up on this one")
		if timer != nil && !timer.Stop() {
			<-timer.C
		}
	case <-dp.stop:
		if timer != nil && !timer.Stop() {
			<-timer.C
		}
	}
}

func (dp *dittoPinger) Ping(pingID uint64, timeout time.Duration, timeoutCount int, reset *resetter) {
	dp.pingHandlingLock.Lock()
	if time.Since(dp.lastPingTime) < minPingInterval {
		dp.log.Debug().
			Uint64("ping_id", pingID).
			Time("last_ping_time", dp.lastPingTime).
			Msg("Skipping ping since last one was too recently")
		dp.pingHandlingLock.Unlock()
		return
	}
	now := time.Now()
	dp.lastPingTime = now
	if dp.oldestPingTime.IsZero() {
		dp.oldestPingTime = now
	}
	pingChan, err := dp.client.NotifyDittoActivity(dp.log.WithContext(dp.ctx))
	if err != nil {
		logSafeError(dp.log.Error(), err).Uint64("ping_id", pingID).Msg("Error sending ping")
		dp.pingFails++
		dp.client.triggerEvent(&events.PingFailed{
			Error:      fmt.Errorf("failed to notify ditto activity: %w", err),
			ErrorCount: dp.pingFails,
		})
		dp.pingHandlingLock.Unlock()
		return
	}
	dp.pingHandlingLock.Unlock()
	if timeoutCount == 0 {
		dp.WaitForResponse(pingID, now, timeout, timeoutCount, pingChan, reset)
	} else {
		dp.startWorker(func() { dp.WaitForResponse(pingID, now, timeout, timeoutCount, pingChan, reset) })
	}
}

func (dp *dittoPinger) startWorker(run func()) {
	dp.workerMu.Lock()
	if dp.stopping {
		dp.workerMu.Unlock()
		return
	}
	dp.workers.Add(1)
	dp.workerMu.Unlock()
	go func() {
		defer dp.workers.Done()
		run()
	}()
}

func (dp *dittoPinger) stopWorkers() {
	dp.workerMu.Lock()
	dp.stopping = true
	dp.workerMu.Unlock()
	dp.workers.Wait()
}

const DefaultBugleDefaultCheckInterval = 2*time.Hour + 55*time.Minute

func (dp *dittoPinger) Loop() {
	defer dp.stopWorkers()
	var lastDataReceiveCheck time.Time
	for {
		var pingStart time.Time
		interval := time.NewTimer(dp.pingInterval)
		select {
		case <-dp.client.pingShortCircuit:
			interval.Stop()
			pingID := pingIDCounter.Add(1)
			dp.log.Debug().Uint64("ping_id", pingID).Msg("Ditto ping wait short-circuited")
			pingStart = time.Now()
			dp.Ping(pingID, shortPingTimeout, 0, newResetter())
		case <-interval.C:
			pingID := pingIDCounter.Add(1)
			dp.log.Trace().Uint64("ping_id", pingID).Msg("Doing normal ditto ping")
			pingStart = time.Now()
			dp.Ping(pingID, defaultPingTimeout, 0, newResetter())
		case <-dp.stop:
			interval.Stop()
			return
		}
		if dp.client.shouldDoDataReceiveCheck() {
			dp.log.Warn().
				Time("last_data_receive_check", lastDataReceiveCheck).
				Msg("No data received recently, sending extra GET_UPDATES call")
			dp.HandleNoRecentUpdates()
			lastDataReceiveCheck = time.Now()
		} else if time.Since(pingStart) > 5*time.Minute || (time.Since(pingStart) > 1*time.Minute && time.Since(lastDataReceiveCheck) > 30*time.Minute) {
			dp.log.Warn().
				Time("ping_start", pingStart).
				Time("last_data_receive_check", lastDataReceiveCheck).
				Msg("Was disconnected for over a minute, sending extra GET_UPDATES call")
			dp.HandleNoRecentUpdates()
			lastDataReceiveCheck = time.Now()
		}
	}
}

func (dp *dittoPinger) HandleNoRecentUpdates() {
	dp.client.triggerEvent(&events.NoDataReceived{})
	err := dp.client.sessionHandler.sendMessageNoResponse(dp.log.WithContext(dp.ctx), SendMessageParams{
		Action:    gmproto.ActionType_GET_UPDATES,
		OmitTTL:   true,
		RequestID: dp.client.sessionHandler.sessionID,
	})
	if err != nil {
		logSafeError(dp.log.Error(), err).Msg("Failed to send extra GET_UPDATES call")
	} else {
		dp.log.Debug().Msg("Sent extra GET_UPDATES call")
	}
}

func (c *Client) shouldDoDataReceiveCheck() bool {
	c.nextDataReceiveCheckLock.Lock()
	defer c.nextDataReceiveCheckLock.Unlock()
	if time.Until(c.nextDataReceiveCheck) <= 0 {
		c.nextDataReceiveCheck = time.Now().Add(c.dataReceiveCheckInterval)
		return true
	}
	return false
}

func (c *Client) bumpNextDataReceiveCheck(after time.Duration) {
	c.nextDataReceiveCheckLock.Lock()
	if time.Until(c.nextDataReceiveCheck) < after {
		c.nextDataReceiveCheck = time.Now().Add(after)
	}
	c.nextDataReceiveCheckLock.Unlock()
}

func tryReadBody(resp io.ReadCloser) []byte {
	data, _ := readProviderResponseBounded(context.Background(), resp, 64<<10)
	_ = resp.Close()
	return data
}

func (c *Client) doLongPoll(loggedIn, background bool, onFirstConnect func()) bool {
	return c.doLongPollContext(context.Background(), loggedIn, background, onFirstConnect)
}

func (c *Client) doLongPollContext(ctx context.Context, loggedIn, background bool, onFirstConnect func()) bool {
	c.pollingMu.Lock()
	c.listenID++
	listenID := c.listenID
	c.disconnecting = false
	c.pollingMu.Unlock()
	listenReqID := uuid.NewString()

	log := c.Logger.With().Int("listen_id", listenID).Logger()
	defer func() {
		log.Debug().Msg("Long polling stopped")
	}()
	ctx = log.WithContext(ctx)
	log.Debug().Str("listen_uuid", listenReqID).Msg("Long polling starting")

	if loggedIn {
		stopDittoPinger := make(chan struct{})
		pingerDone := make(chan struct{})
		go (&dittoPinger{
			pingInterval:      c.pingInterval,
			alertTimeoutCount: c.alertTimeoutCount,
			stop:              stopDittoPinger,
			ctx:               ctx,
			log:               &log,
			client:            c,
		}).loopWithDone(pingerDone)
		defer func() {
			close(stopDittoPinger)
			<-pingerDone
		}()
	}

	errorCount := uint(1)
	for ctx.Err() == nil && c.isListenGeneration(listenID) {
		err := c.refreshAuthTokenContext(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				return true
			}
			if exhttp.IsNetworkError(err) {
				if loggedIn {
					c.triggerEvent(&events.ListenTemporaryError{Error: fmt.Errorf("failed to refresh auth token: %w", err)})
				}
				errorCount++
				if background {
					if errorCount >= 3 {
						return false
					}
				}
				logSafeError(log.Error(), err).Msg("Error refreshing auth token, retrying with jitter")
				if !c.waitLongPollRetry(ctx, errorCount) {
					return true
				}
				continue
			}
			logSafeError(log.Error(), err).Msg("Error refreshing auth token")
			if loggedIn {
				c.triggerEvent(&events.ListenFatalError{Error: fmt.Errorf("failed to refresh auth token: %w", err)})
			}
			return false
		}
		log.Trace().Msg("Starting new long-polling request")
		auth := c.AuthData.Snapshot()
		payload := &gmproto.ReceiveMessagesRequest{
			Auth: &gmproto.AuthMessage{
				RequestID:        listenReqID,
				TachyonAuthToken: auth.TachyonAuthToken,
				Network:          auth.AuthNetwork(),
				ConfigVersion:    util.ConfigMessage,
			},
			Unknown: &gmproto.ReceiveMessagesRequest_UnknownEmptyObject2{
				Unknown: &gmproto.ReceiveMessagesRequest_UnknownEmptyObject1{},
			},
		}
		url := util.ReceiveMessagesURL
		if auth.HasCookies() {
			url = util.ReceiveMessagesURLGoogle
		}
		resp, err := c.makeProtobufHTTPRequestContext(ctx, url, payload, ContentTypePBLite, true)
		auth.ClearSecrets()
		if err != nil {
			if ctx.Err() != nil {
				return true
			}
			if loggedIn {
				c.triggerEvent(&events.ListenTemporaryError{Error: err})
			}
			errorCount++
			if background {
				if errorCount >= 3 {
					return false
				}
			}
			logSafeError(log.Error(), err).Msg("Error making listen request, retrying with jitter")
			if !c.waitLongPollRetry(ctx, errorCount) {
				return true
			}
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			body := tryReadBody(resp.Body)
			zeroBytes(body)
			log.Error().
				Int("status_code", resp.StatusCode).
				Msg("Error making listen request")
			if loggedIn {
				c.triggerEvent(&events.ListenFatalError{Error: events.HTTPError{Action: "polling", StatusCode: resp.StatusCode, Classification: "authorization"}})
			}
			return false
		} else if resp.StatusCode >= 400 {
			body := tryReadBody(resp.Body)
			zeroBytes(body)
			if loggedIn {
				c.triggerEvent(&events.ListenTemporaryError{Error: events.HTTPError{Action: "polling", StatusCode: resp.StatusCode, Classification: classifyHTTPStatus(resp.StatusCode)}})
			}
			errorCount++
			if background {
				if errorCount >= 3 {
					return false
				}
			}
			log.Debug().
				Int("statusCode", resp.StatusCode).
				Msg("Error in long polling, retrying with jitter")
			if !c.waitLongPollRetry(ctx, errorCount) {
				return true
			}
			continue
		}
		if errorCount > 0 {
			errorCount = 0
			if loggedIn {
				c.triggerEvent(&events.ListenRecovered{})
			}
		}
		log.Debug().Int("statusCode", resp.StatusCode).Msg("Long polling opened")
		if ctx.Err() != nil || !c.isListenGeneration(listenID) {
			_ = resp.Body.Close()
			return true
		}
		c.setLongPollingConnection(resp.Body)
		if onFirstConnect != nil {
			if ctx.Err() != nil {
				_ = resp.Body.Close()
				return true
			}
			onFirstConnect()
			onFirstConnect = nil
		}
		cleanClose := c.readLongPollContext(ctx, &log, resp.Body, background)
		c.clearLongPollingConnection(resp.Body)
		if background {
			return cleanClose
		}
	}
	return true
}

func (c *Client) readLongPoll(log *zerolog.Logger, rc io.ReadCloser, background bool) bool {
	return c.readLongPollContext(context.Background(), log, rc, background)
}

func (c *Client) readLongPollContext(ctx context.Context, log *zerolog.Logger, rc io.ReadCloser, background bool) bool {
	defer rc.Close()
	reader := bufio.NewReader(rc)
	buffer := make([]byte, 64<<10)
	prefix := make([]byte, 2)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		logSafeError(log.Error(), err).Msg("Error reading opening bytes")
		return false
	} else if !bytes.Equal(prefix, []byte("[[")) {
		log.Error().Msg("Opening is not [[")
		return false
	}
	var closeIn *time.Timer
	receivedEvents := false
	onRead := func() {
		if closeIn == nil {
			return
		}
		if receivedEvents {
			closeIn.Reset(3 * time.Second)
		} else {
			closeIn.Reset(5 * time.Second)
		}
	}
	if background {
		closeIn = time.NewTimer(10 * time.Second)
		closeStop := make(chan struct{})
		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			select {
			case <-closeIn.C:
				c.closeLongPolling()
			case <-closeStop:
			case <-ctx.Done():
			}
		}()
		defer func() {
			close(closeStop)
			closeIn.Stop()
			<-closeDone
		}()
	}
	scanner := longPollFrameScanner{}
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			onRead()
			parseErr := scanner.Feed(buffer[:n], func(frame []byte) error {
				msg := &gmproto.LongPollingPayload{}
				if err := pblite.Unmarshal(frame, msg); err != nil {
					logSafeError(log.Error(), err).Msg("Error deserializing pblite message")
					return nil
				}
				c.emitLifecycleActivity(lifecycleActivityFrame)
				switch {
				case msg.GetData() != nil:
					c.handleRPCMsgEnvelopeContext(ctx, msg.GetData(), frame)
					receivedEvents = true
					onRead()
				case msg.GetAck() != nil:
					level := zerolog.TraceLevel
					if msg.GetAck().GetCount() > 0 {
						level = zerolog.DebugLevel
					}
					log.WithLevel(level).Int32("count", msg.GetAck().GetCount()).Msg("Got startup ack count message")
					c.setSkipCount(int(msg.GetAck().GetCount()))
				case msg.GetStartRead() != nil:
					log.Trace().Msg("Got startRead message")
				case msg.GetHeartbeat() != nil:
					log.Trace().Msg("Got heartbeat message")
				default:
					logSafeBytes(log.Warn(), "provider_frame", frame).Msg("Got unknown message")
				}
				return nil
			})
			if parseErr != nil {
				logSafeError(log.Warn(), parseErr).Msg("Invalid long-poll framing")
				return false
			}
		}
		if readErr != nil {
			var logEvt *zerolog.Event
			if (errors.Is(readErr, io.EOF) && scanner.ended) || ctx.Err() != nil || c.isDisconnecting() {
				logEvt = log.Trace()
			} else {
				logEvt = log.Warn()
			}
			logSafeError(logEvt, readErr).Msg("Stopped reading data from server")
			return receivedEvents && scanner.ended
		}
	}
}

type longPollFrameScanner struct {
	frame             []byte
	depth             int
	inString          bool
	escaped           bool
	started           bool
	firstCloseBracket bool
	ended             bool
}

func (scanner *longPollFrameScanner) Feed(input []byte, emit func([]byte) error) error {
	for _, value := range input {
		if scanner.ended {
			if !longPollSeparator(value, false) {
				return errors.New("bytes after provider stream end marker")
			}
			continue
		}
		if !scanner.started {
			if scanner.firstCloseBracket {
				if value != ']' {
					return errors.New("invalid provider stream end marker")
				}
				scanner.firstCloseBracket = false
				scanner.ended = true
				continue
			}
			if longPollSeparator(value, true) {
				continue
			}
			if value == ']' {
				scanner.firstCloseBracket = true
				continue
			}
			if value != '[' && value != '{' {
				return errors.New("provider frame is not a JSON container")
			}
			scanner.started = true
			scanner.depth = 1
			scanner.frame = append(scanner.frame[:0], value)
			continue
		}

		if len(scanner.frame) >= maxLongPollEnvelopeBytes {
			return errors.New("provider envelope exceeded durable inbox limit")
		}
		scanner.frame = append(scanner.frame, value)
		if scanner.inString {
			if scanner.escaped {
				scanner.escaped = false
			} else if value == '\\' {
				scanner.escaped = true
			} else if value == '"' {
				scanner.inString = false
			}
			continue
		}
		switch value {
		case '"':
			scanner.inString = true
		case '[', '{':
			scanner.depth++
		case ']', '}':
			scanner.depth--
			if scanner.depth < 0 {
				return errors.New("invalid provider JSON container")
			}
			if scanner.depth == 0 {
				if err := emit(scanner.frame); err != nil {
					return err
				}
				scanner.frame = nil
				scanner.started = false
			}
		}
	}
	return nil
}

func longPollSeparator(value byte, comma bool) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || comma && value == ','
}

func (c *Client) closeLongPolling() {
	c.pollingMu.Lock()
	conn := c.longPollingConn
	listenID := c.listenID
	c.listenID++
	c.disconnecting = true
	c.longPollingConn = nil
	c.pollingMu.Unlock()
	if conn != nil {
		c.Logger.Debug().Int("current_listen_id", listenID).Msg("Closing long polling connection manually")
		_ = conn.Close()
	}
}

func (c *Client) isListenGeneration(generation int) bool {
	c.pollingMu.Lock()
	defer c.pollingMu.Unlock()
	return c.listenID == generation
}

func (c *Client) isDisconnecting() bool {
	c.pollingMu.Lock()
	defer c.pollingMu.Unlock()
	return c.disconnecting
}

func (c *Client) setLongPollingConnection(conn io.Closer) {
	c.pollingMu.Lock()
	c.longPollingConn = conn
	c.disconnecting = false
	c.pollingMu.Unlock()
}

func (c *Client) clearLongPollingConnection(conn io.Closer) {
	c.pollingMu.Lock()
	if c.longPollingConn == conn {
		c.longPollingConn = nil
	}
	c.pollingMu.Unlock()
}

func (dp *dittoPinger) loopWithDone(done chan<- struct{}) {
	defer close(done)
	dp.Loop()
}
