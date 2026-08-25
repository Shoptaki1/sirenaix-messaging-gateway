# Physical device matrix

Status: **not executed**. No physical phone, carrier, SIM, Google Messages,
SMS, MMS, or RCS result is claimed by this document.

The v1 pilot uses one SirenaIX gateway replica. A tenant may pair multiple
physical Android phones, so the matrix must include tenants with one phone and
tenants with several phones. Every phone remains online, runs the official
Google Messages app, and is recorded as a distinct gateway connection.

## Required coverage

The pilot expands only after the preceding wave passes its exit gates:

| Wave | Concurrent physical phones | Minimum observation | Required mix |
|---|---:|---:|---|
| 1 | 5 | 72-hour authorization soak | At least two manufacturers, two Android versions, two carriers, SMS and image/MMS; RCS where carrier-supported |
| 2 | 20 | 72-hour authorization soak | Add multi-phone tenants, prepaid/postpaid carriers, Wi-Fi/cellular transitions, and at least three dual-SIM phones |
| 3 | 50 | 7 continuous days | Preserve prior coverage; include normal restarts and planned phone/network interruptions |

The 72-hour clock restarts for a phone after an unplanned re-pair, Google
Messages data reset, OS upgrade, or gateway session recovery that changes its
authorization state. Wave 3 must retain all 50 phones for the full seven-day
observation; replacing a phone does not preserve that phone's elapsed time.

## Evidence record

Use one row per tested phone/configuration. Store evidence in an access-
controlled pilot system and link only to a redacted export. Never record phone
numbers, QR codes, pairing data, message bodies, contact data, or session
credentials in this repository.

| Evidence ID | Wave | Phone manufacturer/model | Android version/build | Google Messages version | Carrier | SIM configuration | RCS state | SMS result | Image/MMS result | Explicit line-route result | Source commit | Test date (UTC) | Evidence link | Status/notes |
|---|---:|---|---|---|---|---|---|---|---|---|---|---|---|---|
| _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | single/dual/eSIM | unavailable/configuring/connected | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | Not executed |

## Dual-SIM interpretation

SirenaIX preserves provider-discovered line identifiers and can request an
explicit outgoing line when Google Messages exposes a usable route. Android,
Google Messages, carrier, and device behavior decide whether both SIMs are
actually distinguishable and selectable. Therefore:

- discovery of two SIMs is not proof that outbound routing works;
- each SIM needs a separate send-and-receive test after every relevant app,
  OS, carrier, or SIM configuration change;
- inbound events that do not identify a line must remain marked ambiguous;
- the AI must not guess a line, retry an uncertain send, or claim dual-SIM
  support for a matrix row that has not passed explicit routing tests.

Any ambiguous or wrong-line result triggers the pilot hard stop in
[pilot-plan.md](pilot-plan.md).
