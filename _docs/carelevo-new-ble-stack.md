# CareLevo — New BLE Stack: Architecture, Status & Migration Roadmap

Coroutine + `CompletableDeferred` BLE client built alongside the legacy Rx driver; smoke-test reachable via a dev button, no production integration yet.

## What exists

A new coroutine-first BLE stack built alongside the legacy `PublishSubject + blockingFirst` driver. Lives in `pump/carelevo/src/main/kotlin/app/aaps/pump/carelevo/ble/`:

```
ble/
├── BleClient.kt              — interface + BleCommand + BleResponse + UnsolicitedMessage + BleDisconnectedException
├── BleClientImpl.kt          — Mutex-serialized; registers @Volatile waiter BEFORE writing; opcode + optional correlationByte match
├── commands/
│   ├── MacAddressCommand.kt  — 0x3B→0x9B, read-only, uses clean uppercase hex (not legacy 0xAA0xBB format)
│   └── ImmediateBolusCommand.kt — 0x24→0x84, actionId correlation, BigDecimal HALF_UP rounding
└── gatt/
    ├── GattConnection.kt     — abstraction: SharedFlow<GattEvent> + write/discover/enableNotifications/close
    └── AndroidGattConnection.kt — Android BluetoothGatt wrapper, **UNTESTED ON HARDWARE**

compose/smoketest/
└── CarelevoBleSmokeTest.kt   — runMacAddressSmokeTest() + CarelevoBleSmokeTestDialog Composable
```

Tests in `pump/carelevo/src/test/kotlin/app/aaps/pump/carelevo/ble/`:
- `BleClientContractTest.kt` — 11 tests (spec-as-tests)
- `commands/MacAddressCommandTest.kt` — 5 tests
- `commands/ImmediateBolusCommandTest.kt` — 15 tests
- `gatt/FakeGattConnection.kt` — scriptable test fixture

**Why:** the legacy driver's `PublishSubject + blockingFirst` correlation races (response arrives before subscribe → event lost → forever hang). New stack eliminates this by registering the `CompletableDeferred` waiter *before* calling `gatt.writeCharacteristic`. Proven by contract test `10 response delivered synchronously during writeCharacteristic is not lost`.

## Correlation rules

- Primary: opcode match (`BleCommand.expectedResponseOpcode` vs `payload[0]`)
- Secondary (bolus-family only): `correlationByte` == `payload[1]` (actionId echo check — rejects stale/mis-routed responses)
- Unsolicited: any notification that doesn't match the active waiter → `unsolicitedEvents: SharedFlow<UnsolicitedMessage>`
- Disconnect: `ConnectionStateChanged(DISCONNECTED)` aborts waiter with `BleDisconnectedException` and clears `waiter` reference immediately so late notifications route to unsolicited instead of being dropped

## Integration status

**Zero production call sites.** `BleClientImpl` and `AndroidGattConnection` are compiled into the APK but never invoked by any normal user flow. The only entry point is the dev smoke-test dialog.

Smoke test hook: `CarelevoPatchFlowStep01Start.kt` has a "[Dev] BLE smoke test" TextButton near the bottom. Tap opens `CarelevoBleSmokeTestDialog`, user enters MAC, tap Run, dialog opens its own `AndroidGattConnection`, calls `MacAddressCommand`, displays result. Dialog is reachable only on the pairing start screen (when no patch is paired).

## Patterns NOT yet supported by BleClient

Two legitimate protocol patterns not expressible with current `request(cmd): R` API:

1. **Multi-response per request.** `PatchInformationInquiry` (0x33) → pump sends RPT1 (0x93) + RPT2 (0x94). Current `BleCommand` has singular `expectedResponseOpcode`. Needs `requestMultiple` or similar.
2. **Streaming / progress events.** `SafetyCheck` (0x12) emits progress (REP_REQUEST) → eventually SUCCESS. Needs `Flow<R>` return or progress callback.

Design extension required before porting these commands. Simple single-response commands (SetTime, TempBasal, BolusCancel, etc.) work with current API unchanged.

## Migration roadmap (next steps, in rough order)

1. **Hardware smoke test** — unpair a patch, build+flash, run the dev dialog with an unbonded pump. If MAC comes back correctly, `AndroidGattConnection` is validated on real hardware. This is the critical-path gate before any wider rollout.
2. **Extend BleClient API** for multi-response + streaming patterns (before writing 30 commands that would then need refactoring).
3. **Bulk-write remaining commands** — ~30 total, each ~70 lines, fully unit-testable against `FakeGattConnection`.
4. **Migrate first repository** behind a feature flag. Suggested start: a non-dosing read-only repo (patch info), so any bug has low blast radius. Keep the legacy path available for instant rollback.
5. **End-to-end equivalence tests** — small set (~6) at `CarelevoPumpPlugin` level with faked `GattConnection` below, real everything above. Run against both legacy and new stacks to prove equivalent `PumpEnactResult` outputs.
6. **State consolidation** — collapse `BehaviorSubject<Optional<X>>` + `StateFlow<X?>` + LiveData triple state in `CarelevoPatch` to StateFlow-only. Bridge extension `StateFlow.asBehaviorSubject()` keeps legacy consumers alive during migration.
7. **Coordinator simplification** — once repositories are suspend, coordinators become thin suspend orchestrators. Single `runBlocking(IO)` at Pump-interface boundary in `CarelevoPumpPlugin`.
8. **Drop RxJava dependency** — final cleanup once all consumers migrated.

Each phase ships a working driver; no long-lived broken intermediate state.

## Known non-issues (reviewed, deliberately deferred)

- `FakeGattConnection._writes` + `ArrayDeque`s not thread-safe — safe today because `BleClientImpl.requestMutex` serializes callers. Revisit if future tests bypass `BleClient`.
- `MacAddressResponse` hex format diverges from legacy `0xAA0xBB...` — intentional; no consumers wired yet; repository migration will add a formatter if needed.
- `AndroidGattConnection` doesn't include: reconnect/retry policy, MTU negotiation, bonding coordination, gatt refresh on abnormal disconnect, OEM quirk workarounds. Legacy `CarelevoBleMangerImpl` has all of these — `AndroidGattConnection` is a minimal skeleton for the request/response path only.

## Why this matters

The disabled test `CarelevoConnectNewPatchUseCaseTest.execute_retries_round_when_serial_is_empty` — the one preserved as `@Disabled` — is exactly the pattern this new stack fixes. Once repositories migrate to `BleClient`, the test can be rewritten (each round uses a fresh `CompletableDeferred`, no shared subject state) and re-enabled.
