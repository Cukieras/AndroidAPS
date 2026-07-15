package app.aaps.pump.carelevo.ble

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.carelevo.ble.commands.InfusionInfoCommand
import app.aaps.pump.carelevo.ble.commands.InfusionInfoResponse
import app.aaps.pump.carelevo.ble.commands.PatchInfoCommand
import app.aaps.pump.carelevo.ble.commands.PatchInfoResponse
import app.aaps.pump.carelevo.ble.gatt.BleTransportGattConnection
import app.aaps.pump.carelevo.ble.gatt.GattConnState
import app.aaps.pump.carelevo.ble.gatt.GattEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import java.util.UUID
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

/**
 * Owns one full new-transport BLE session for the Phase-2 [BleClient] stack: connect → discover
 * services → enable notifications → run one [BleClient] exchange → close. This is the reusable
 * session backbone the wider migration builds on (see `_docs/carelevo-new-ble-stack.md`).
 *
 * Each call builds a **fresh** [BleTransportGattConnection] + [BleClientImpl] + [CoroutineScope] and
 * tears them down when done. That is deliberate: [BleTransportGattConnection.close] is one-shot (it
 * latches `closed` and releases the transport's single listener slot), so a long-lived shared
 * instance would brick after its first session. Per-session-fresh keeps each session independent.
 *
 * The new transport opens its OWN GATT, independent of the legacy `CarelevoBleMangerImpl`, so a
 * session must not run concurrently with the legacy link (two GATT clients to one patch = the
 * status-133 collision). The caller (a flag-gated customCommand) is responsible for dropping the
 * legacy link and suppressing its reconnect before invoking a session.
 */
@Singleton
class CarelevoBleSession @Inject constructor(
    private val transport: CarelevoBleTransport,
    @Named("characterRx") private val writeUuid: UUID,
    @Named("characterTx") private val notifyUuid: UUID,
    private val aapsLogger: AAPSLogger
) {

    /**
     * Connect to [address], read Patch Info (0x33 → 0x93 RPT1 + 0x94 RPT2), and close.
     *
     * @throws IllegalArgumentException if the BLE stack refuses to start the connection.
     * @throws kotlinx.coroutines.TimeoutCancellationException on connect-handshake or read timeout.
     * Always closes the connection and cancels the session scope, even on failure.
     */
    /** Read Patch Info (0x33 → 0x93 RPT1 + 0x94 RPT2). */
    suspend fun readPatchInfo(address: String): PatchInfoResponse =
        withSession(address, "patch info") { it.requestMultiple(PatchInfoCommand()) }

    /** Read Infusion Info (0x31 → 0x91) — the periodic status read (reservoir, totals, pump state). */
    suspend fun readInfusionInfo(address: String): InfusionInfoResponse =
        withSession(address, "infusion info") { it.request(InfusionInfoCommand()) }

    /** Run any single-response [command] (write or read) on a fresh new-transport session. */
    suspend fun <R : BleResponse> runSingle(address: String, command: BleCommand<R>): R =
        withSession(address, command::class.simpleName ?: "command") { it.request(command) }

    /**
     * Open a fresh connection, run [block] against the [BleClient], and close. Each call gets its own
     * adapter+client+scope — see the class KDoc for why (one-shot [BleTransportGattConnection.close]).
     */
    private suspend fun <R> withSession(address: String, label: String, block: suspend (BleClient) -> R): R {
        // BluetoothAdapter.getRemoteDevice requires an UPPERCASE MAC (lowercase throws
        // IllegalArgumentException); the stored address is lowercase, so normalize here.
        val mac = address.uppercase()
        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val gatt = BleTransportGattConnection(transport, writeUuid, notifyUuid, scope)
        val client: BleClient = BleClientImpl(gatt, writeUuid, notifyUuid, scope)
        try {
            open(gatt, mac)
            aapsLogger.debug(LTag.PUMPCOMM, "bleSession: reading $label")
            return withTimeout(READ_TIMEOUT_MS) { block(client) }
        } finally {
            gatt.close()
            scope.cancel()
        }
    }

    private suspend fun open(gatt: BleTransportGattConnection, address: String) = coroutineScope {
        // Subscribe to the CONNECTED event BEFORE calling connect() so the state change cannot race
        // ahead of our collector. UNDISPATCHED runs the async body up to the flow subscription
        // synchronously, guaranteeing the subscription is live before connect() fires.
        val connected = async(start = CoroutineStart.UNDISPATCHED) {
            withTimeout(CONNECT_TIMEOUT_MS) {
                gatt.events
                    .filterIsInstance<GattEvent.ConnectionStateChanged>()
                    .first { it.state == GattConnState.CONNECTED }
            }
        }
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: connecting to $address")
        require(gatt.connect(address)) { "bleSession: connect() refused for $address" }
        connected.await()
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: connected; discovering services")
        gatt.discoverServices()
        gatt.enableNotifications(notifyUuid)
        aapsLogger.debug(LTag.PUMPCOMM, "bleSession: notifications enabled")
    }

    private companion object {

        const val CONNECT_TIMEOUT_MS = 20_000L
        const val READ_TIMEOUT_MS = 15_000L
    }
}
