# SirenaIX messaging gateway pilot plan

Status: **not started**. The physical-phone results and soak periods below are
release gates, not completed claims.

## Pilot boundary

The v1 pilot runs a single gateway replica and an online Android phone with the
official Google Messages app for every connection. Multiple phones per tenant
are supported and must be represented in waves 2 and 3. Replica failover and
active-active high availability are outside this pilot; they require a later
multi-replica exercise.

The customer's AI is outside the gateway. During the pilot it may consume
redacted test events only after gateway durability, tenant isolation, and
outbound idempotency checks pass.

## Entry gates

Before wave 1:

1. The exact source commit is deployed through the release pipeline.
2. Database migrations, backups, restore rehearsal, KMS access, webhook/Kafka
   credentials, rate limits, and operator alerts pass in the pilot environment.
3. The Linux race-enabled 1,000-actor simulation passes for that commit and
   its JSON artifact is retained.
4. Test tenants, phone owners, approved recipients, carriers, and evidence
   access are documented without placing personal data in GitHub.
5. Operators rehearse quarantine, reauthorization, uncertain-send, secret
   rotation, and full pilot shutdown procedures.

## Waves and promotion

| Wave | Phones | Required run | Promotion gate |
|---|---:|---:|---|
| 1 | 5 | 72 hours continuously authorized | All five rows complete; zero hard stops; SMS, image/MMS, reconnect, restart, and available RCS routes verified |
| 2 | 20 | 72 hours continuously authorized | All 20 rows complete; multi-phone tenants and dual-SIM limitations exercised; zero hard stops |
| 3 | 50 | 7 continuous days at 50 | All 50 rows complete for seven days; zero hard stops; evidence and incident review signed off |

Promotion is manual. A green dashboard alone cannot promote a wave. Planned
maintenance pauses the observation clock for affected phones; an unplanned
authorization reset restarts that phone's 72-hour clock. Wave 3 succeeds only
after all 50 connections complete the same seven-day window.

## Quantitative responsiveness and queue gates

These gates address gateway delay, especially the inconsistent fast/slow
behavior that prompted this work. They do not claim control over radio,
carrier, recipient-device, or final delivery latency.

### Measurement boundaries

Measure four paths independently and never combine them into one percentile:

1. **Inbound webhook:** durable provider-frame commit to the configured webhook
   returning an accepted 2xx response.
2. **Inbound Kafka:** durable provider-frame commit to the broker acknowledging
   the gateway's event write. Consumer processing time is separate.
3. **Outbound API:** durable API command acceptance to the Google Messages
   provider returning an acceptance response for that exact idempotency key.
4. **Outbound Kafka:** durable Kafka command acceptance by the gateway to the
   same Google-provider acceptance response.

Use timestamps from the single gateway replica's monotonic clock and correlate
each interval with the durable event/command ID. Classify observations as text
or image and as normal, reconnect, or backfill before calculating percentiles.
`Normal` means the actor was continuously ready with no backlog. `Reconnect`
starts when an actor becomes ready after an interruption; `backfill` is an
inbound historical-page cohort. Calculate nearest-rank p50/p95/p99 for each
path/class, record sample count, and retain a redacted histogram rather than
message-level data.

The inbound interval starts after the frame commits, so separately record the
diagnostic provider-observation lag from gateway frame receipt where provider
timestamps make that possible. The outbound interval ends when Google accepts
the provider operation. Neither interval includes carrier delivery, recipient
receipt, or read receipt. Report those end-to-end observations separately and
do not fail the gateway for carrier time.

### Promotion thresholds

The following limits apply independently to all applicable inbound and
outbound paths. A row must pass p50, p95, and p99; averaging tenants, phones,
sinks, directions, or payload classes is prohibited.

| Mode | Payload | p50 | p95 | p99 |
|---|---|---:|---:|---:|
| Normal | Text | <= 500 ms | <= 2 s | <= 5 s |
| Normal | Image/media | <= 1.5 s | <= 5 s | <= 15 s |
| Reconnect/backfill | Text | <= 2 s | <= 10 s | <= 30 s |
| Reconnect/backfill | Image/media | <= 5 s | <= 30 s | <= 60 s |

Each wave needs at least 1,000 normal-text and 200 normal-image completed
observations per applicable path, with at least 50 text and 10 image samples
from every capable phone. Reconnect/backfill evaluation needs at least 200 text
and 100 image observations per applicable path across at least 20 controlled
recovery/backfill episodes; every phone must contribute at least two recovery
episodes and one media observation where supported. A path without the minimum
sample cannot pass promotion and must be recorded as insufficient evidence.

Record eligible queue depth, configured queue capacity, and oldest eligible
item age every 15 seconds, split by tenant connection and direction. Promotion
also requires:

- normal-operation p95 queue depth below 50% of the configured cap and oldest
  eligible item age <= 30 seconds in at least 99% of samples;
- at actor-ready plus 60 seconds, the reconnect/backfill cohort's oldest
  eligible item age <= 60 seconds and at least 95% of queued text completed;
- at actor-ready plus 120 seconds, at least 95% of queued images completed;
- every pre-ready cohort item accepted by a sink/provider or placed in an
  explicit terminal state within five minutes after recovery; and
- zero confirmed duplicate provider acceptances, duplicate downstream events,
  uncertain sends, wrong-line sends, or silent queue drops.

### Performance hard stops

In addition to the immediate correctness stops below, stop the wave when any
applicable path breaches either p95 or p99 below for three consecutive
five-minute windows. Each evaluated window must contain at least 100 text or 20
image completions; sparse windows block promotion but do not manufacture a
percentile incident.

| Mode | Payload | Hard-stop p95 | Hard-stop p99 |
|---|---|---:|---:|
| Normal | Text | > 5 s | > 15 s |
| Normal | Image/media | > 15 s | > 45 s |
| Reconnect/backfill | Text | > 30 s | > 60 s |
| Reconnect/backfill | Image/media | > 60 s | > 120 s |

Also stop immediately if an eligible item remains queued more than five
minutes after its actor is ready, the queue reaches its configured hard cap or
drops an item, the oldest recovery item is still over 60 seconds old at the
ready-plus-60-second checkpoint, or any duplicate/uncertain/wrong send occurs.
Queue depth at or above 80% of its configured cap for three consecutive
one-minute samples is a hard stop before overflow.

These values are conservative v1 pilot defaults. A tenant may tighten them.
Relaxing a limit requires a reviewed change record, documented capacity reason,
new baseline evidence, and restart of the active wave; an operator cannot tune
a threshold merely to turn a failed wave green.

## Exact hard stops

Stop new outbound sends, quarantine affected connections, preserve redacted
evidence, and page the pilot owner immediately when any condition below occurs:

- **Cross-tenant:** any message, media, contact, label, connection, line,
  credential, metric label, log field, webhook, or Kafka event is visible to or
  controllable by the wrong tenant.
- **Lost ingress:** any provider-delivered message or media that was reported
  durable cannot be retrieved, projected, or replayed from the gateway's
  authoritative records.
- **Wrong ACK:** any provider response is acknowledged before its durable
  commit, after a recorded conflict, for the wrong tenant/connection, or after
  its actor loses the lease fence.
- **Secret in logs/evidence:** any session credential, pairing/QR material,
  API credential, webhook secret, encryption key, full phone number, message
  body, contact data, or unredacted media appears in logs, metrics, CI output,
  GitHub artifacts, screenshots, or pilot exports.
- **Mass reauthorization:** two or more otherwise healthy pilot phones require
  unplanned reauthorization within any rolling 30-minute window, or at least
  10% of the active wave requires it within 24 hours, whichever happens first.
- **Uncertain or wrong send:** any outbound command cannot be classified from
  durable state as safe-to-send, confirmed, or terminal without risking a
  duplicate; or any message/media is sent to the wrong recipient, conversation,
  phone, SIM/line, or tenant.
- **Responsiveness or queue safety:** any performance hard stop above is met,
  including sustained latency, a five-minute eligible item, imminent queue
  overflow, a dropped item, or failure to drain the recovery cohort.

These are zero-tolerance pilot gates. An operator may resume only after the
cause is understood, affected data and credentials are contained, a regression
test exists where possible, the fix passes independent review, and the wave is
formally restarted or re-baselined.

## Per-wave exercises

- Pair, restart the gateway, restart the phone, and verify durable session
  recovery without routine re-pairing.
- Receive and send short/long SMS, Unicode, group messages where supported,
  images/MMS, and RCS text/media where the carrier reports RCS connected.
- Verify contact sync and server-managed alias/label updates without replacing
  the user's phone address book unexpectedly.
- Exercise Wi-Fi loss, cellular loss, airplane mode, battery optimization, app
  update, and controlled reconnect storms.
- On dual-SIM devices, test each provider-exposed line explicitly and preserve
  ambiguity when the provider does not expose a reliable route.
- Verify webhook/Kafka replay, idempotency, rate limiting, quarantine,
  reauthorization, retention, deletion, alerts, and redacted audit evidence.

Record every configuration in [device-matrix.md](device-matrix.md) and every
automated result under [test-results](test-results/README.md).
