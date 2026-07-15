package app.aaps.pump.carelevo

import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.content.Context
import android.content.IntentFilter
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ProcessLifecycleOwner
import app.aaps.core.data.plugin.PluginType
import app.aaps.core.data.pump.defs.ManufacturerType
import app.aaps.core.data.pump.defs.PumpDescription
import app.aaps.core.data.pump.defs.PumpType
import app.aaps.core.data.pump.defs.TimeChangeType
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.plugin.PluginDescription
import app.aaps.core.interfaces.profile.Profile
import app.aaps.core.interfaces.profile.ProfileFunction
import app.aaps.core.interfaces.protection.ProtectionCheck
import app.aaps.core.interfaces.pump.BlePreCheck
import app.aaps.core.interfaces.pump.DetailedBolusInfo
import app.aaps.core.interfaces.pump.Pump
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.pump.PumpInsulin
import app.aaps.core.interfaces.pump.PumpPluginBase
import app.aaps.core.interfaces.pump.PumpProfile
import app.aaps.core.interfaces.pump.PumpRate
import app.aaps.core.interfaces.pump.PumpSync
import app.aaps.core.interfaces.pump.defs.fillFor
import app.aaps.core.interfaces.queue.CommandQueue
import app.aaps.core.interfaces.queue.CustomCommand
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.sharedPreferences.SP
import app.aaps.core.interfaces.ui.IconsProvider
import app.aaps.core.interfaces.utils.fabric.FabricPrivacy
import app.aaps.core.keys.DoubleKey
import app.aaps.core.keys.IntKey
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.keys.interfaces.withEntries
import app.aaps.core.ui.compose.icons.IcPluginCarelevo
import app.aaps.core.ui.compose.preference.PreferenceSubScreenDef
import app.aaps.pump.carelevo.ble.CarelevoBleSession
import app.aaps.pump.carelevo.ble.CarelevoBleSource
import app.aaps.pump.carelevo.ble.data.BondingState.Companion.codeToBondingResult
import app.aaps.pump.carelevo.ble.data.DeviceModuleState.Companion.codeToDeviceResult
import app.aaps.pump.carelevo.ble.data.PeripheralConnectionState
import app.aaps.pump.carelevo.command.CarelevoActivationExecutor
import app.aaps.pump.carelevo.command.CmdTimeZoneUpdate
import app.aaps.pump.carelevo.command.CmdUpdateBuzzer
import app.aaps.pump.carelevo.command.CmdUpdateExpiredThreshold
import app.aaps.pump.carelevo.command.CmdUpdateLowInsulinNotice
import app.aaps.pump.carelevo.command.CmdUpdateMaxBolus
import app.aaps.pump.carelevo.common.CarelevoAlarmNotifier
import app.aaps.pump.carelevo.common.CarelevoObserveReceiver
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.common.keys.CarelevoIntPreferenceKey
import app.aaps.pump.carelevo.common.model.PatchState
import app.aaps.pump.carelevo.compose.CarelevoComposeContent
import app.aaps.pump.carelevo.coordinator.CarelevoBasalProfileUpdateCoordinator
import app.aaps.pump.carelevo.coordinator.CarelevoBolusCoordinator
import app.aaps.pump.carelevo.coordinator.CarelevoConnectionCoordinator
import app.aaps.pump.carelevo.coordinator.CarelevoSettingsCoordinator
import app.aaps.pump.carelevo.coordinator.CarelevoTempBasalCoordinator
import app.aaps.pump.carelevo.data.protocol.parser.CarelevoProtocolParserRegister
import app.aaps.pump.carelevo.domain.model.alarm.CarelevoAlarmInfo
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoInfusionInfoDomainModel
import app.aaps.pump.carelevo.domain.model.userSetting.CarelevoUserSettingInfoDomainModel
import app.aaps.pump.carelevo.domain.type.AlarmCause
import app.aaps.pump.carelevo.domain.type.AlarmType.Companion.isCritical
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Provider
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull

@Singleton
// Settle window between dropping the legacy GATT and the new transport re-dialing the same device
// (BLE stacks dislike an immediate reconnect to a just-closed peripheral). Phase-2.A validation only.
private const val NEW_BLE_SETTLE_MS = 1000L

class CarelevoPumpPlugin @Inject constructor(
    aapsLogger: AAPSLogger,
    rh: ResourceHelper,
    preferences: Preferences,
    commandQueue: CommandQueue,
    private val aapsSchedulers: AapsSchedulers,
    private val rxBus: RxBus,
    private val sp: SP,
    private val fabricPrivacy: FabricPrivacy,
    private val profileFunction: ProfileFunction,
    private val context: Context,
    private val protectionCheck: ProtectionCheck,
    private val blePreCheck: BlePreCheck,
    private val iconsProvider: IconsProvider,
    private var pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoProtocolParserRegister: CarelevoProtocolParserRegister,
    private val carelevoPatch: CarelevoPatch,

    private val carelevoAlarmNotifier: CarelevoAlarmNotifier,
    private val basalProfileUpdateCoordinator: CarelevoBasalProfileUpdateCoordinator,
    private val bolusCoordinator: CarelevoBolusCoordinator,
    private val tempBasalCoordinator: CarelevoTempBasalCoordinator,
    private val connectionCoordinator: CarelevoConnectionCoordinator,
    private val settingsCoordinator: CarelevoSettingsCoordinator,
    private val activationExecutor: CarelevoActivationExecutor
) : PumpPluginBase(
    pluginDescription = PluginDescription()
        .mainType(PluginType.PUMP)
        .composeContent { _ ->
            CarelevoComposeContent(
                aapsLogger = aapsLogger,
                carelevoAlarmNotifier = carelevoAlarmNotifier,
                protectionCheck = protectionCheck,
                blePreCheck = blePreCheck,
                iconsProvider = iconsProvider
            )
        }
        .icon(IcPluginCarelevo)
        .pluginName(R.string.carelevo)
        .shortName(R.string.carelevo_shortname)
        .description(R.string.carelevo_description),
    ownPreferences = listOf(CarelevoBooleanPreferenceKey::class.java, CarelevoIntPreferenceKey::class.java),
    aapsLogger, rh, preferences, commandQueue
), Pump {

    private var bleReceiverDisposable: Disposable? = null
    private val pluginDisposable = CompositeDisposable()

    private var _pumpType: PumpType = PumpType.CAREMEDI_CARELEVO
    private val _pumpDescription = PumpDescription().fillFor(_pumpType)

    private var scope: CoroutineScope? = null
    private var lifecycleObserver: LifecycleEventObserver? = null

    @Inject @Named("characterTx") lateinit var txUuid: UUID

    // Phase-2 new BLE stack session (flag-gated hardware validation). Field-injected — its @Inject
    // constructor holds no eager BLE state, so this is safe even when the flag is off.
    @Inject lateinit var bleSession: CarelevoBleSession

    override suspend fun onStart() {
        super.onStart()

        applyDefaultCageThresholdsIfNeeded()
        registerPreferenceChangeObserver()
        initializeOnStart()
        registerBleReceiverIfNeeded()
        startAlarmObserving()
    }

    override suspend fun onStop() {
        super.onStop()
        aapsLogger.debug(LTag.PUMP, "onStop called")
        settingsCoordinator.clearUserSettings(pluginDisposable)
        pluginDisposable.clear()
        connectionCoordinator.onStop()

        // onStart/onStop run as separate, unjoined pluginScope coroutines (PluginBase), so a fast
        // disable→re-enable can overlap them. Tear down only the generation captured here, and clear
        // a field only if it still points at that generation — never clobber a scope/observer that a
        // concurrent onStart may have just re-created (this fn suspends at withContext below).
        val scopeToCancel = scope
        val observerToRemove = lifecycleObserver

        scopeToCancel?.cancel()
        if (scope === scopeToCancel) scope = null

        carelevoAlarmNotifier.stopObserving()

        observerToRemove?.let { observer ->
            withContext(Dispatchers.Main) {
                ProcessLifecycleOwner.get().lifecycle.removeObserver(observer)
            }
        }
        if (lifecycleObserver === observerToRemove) lifecycleObserver = null
    }

    private fun registerPreferenceChangeObserver() {
        val newScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        scope = newScope

        // Settings pushes go through the queue (connect-before-execute) like every other patch op — a
        // direct fire-and-forget write would silently fail while the pump idle-disconnects between commands.
        preferences.observe(DoubleKey.SafetyMaxBolus)
            .drop(1)
            .onEach { commandQueue.customCommand(CmdUpdateMaxBolus(preferences.get(DoubleKey.SafetyMaxBolus))) }
            .launchIn(newScope)

        preferences.observe(CarelevoIntPreferenceKey.CARELEVO_PATCH_EXPIRATION_REMINDER_HOURS)
            .drop(1)
            .onEach {
                val hours = sp.getInt(CarelevoIntPreferenceKey.CARELEVO_PATCH_EXPIRATION_REMINDER_HOURS.key, 0)
                commandQueue.customCommand(CmdUpdateExpiredThreshold(hours))
            }
            .launchIn(newScope)

        preferences.observe(CarelevoIntPreferenceKey.CARELEVO_LOW_INSULIN_EXPIRATION_REMINDER_HOURS)
            .drop(1)
            .onEach {
                val hours = sp.getInt(CarelevoIntPreferenceKey.CARELEVO_LOW_INSULIN_EXPIRATION_REMINDER_HOURS.key, 0)
                // Zero = reminder off; skip enqueuing so the pump isn't reconnected just to no-op (parity
                // with the old coordinator's zero-skip).
                if (hours != 0) commandQueue.customCommand(CmdUpdateLowInsulinNotice(hours))
            }
            .launchIn(newScope)

        preferences.observe(CarelevoBooleanPreferenceKey.CARELEVO_BUZZER_REMINDER)
            .drop(1)
            .onEach {
                val on = sp.getBoolean(CarelevoBooleanPreferenceKey.CARELEVO_BUZZER_REMINDER.key, false)
                commandQueue.customCommand(CmdUpdateBuzzer(on))
            }
            .launchIn(newScope)

        // Deferred settings-sync recovery: a max-bolus / low-insulin change that couldn't reach the patch
        // (changed while offline, or during a bolus) leaves a needXSyncPatch flag on the stored user
        // settings; when the patch is booted again, push it through the queue. The combiner is PURE and the
        // enqueue runs on IO — replaces the old side-effecting combineLatest in CarelevoPatch that ran on the
        // main thread and called the use cases directly (bypassing the queue).
        pluginDisposable += Observable.combineLatest(
            carelevoPatch.patchState,
            carelevoPatch.infusionInfo,
            carelevoPatch.userSettingInfo
        ) { state, infusion, setting ->
            computeSettingsSyncNeed(state.getOrNull(), infusion.getOrNull(), setting.getOrNull())
        }
            .distinctUntilChanged()
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe { need ->
                newScope.launch {
                    need.maxBolusDose?.let { commandQueue.customCommand(CmdUpdateMaxBolus(it)) }
                    need.lowInsulinHours?.let { commandQueue.customCommand(CmdUpdateLowInsulinNotice(it)) }
                }
            }
    }

    private data class SettingsSyncNeed(val maxBolusDose: Double?, val lowInsulinHours: Int?)

    /** Pure: which deferred patch settings still need pushing (flag set AND the patch is booted). */
    private fun computeSettingsSyncNeed(
        state: PatchState?,
        infusion: CarelevoInfusionInfoDomainModel?,
        setting: CarelevoUserSettingInfoDomainModel?
    ): SettingsSyncNeed {
        if (state !is PatchState.ConnectedBooted || setting == null) return SettingsSyncNeed(null, null)
        // Max-bolus is a device safety cap — do not re-push mid-bolus (mirrors the use case's own guard).
        // "No bolus running" requires BOTH channels idle (the deleted observeSyncPatch had this as `||`,
        // which was true during a single-channel bolus and triggered a spurious mid-bolus reconnect).
        val noBolusRunning = infusion?.extendBolusInfusionInfo == null && infusion?.immeBolusInfusionInfo == null
        val maxBolusDose = if (setting.needMaxBolusDoseSyncPatch && noBolusRunning) setting.maxBolusDose ?: 0.0 else null
        val lowInsulinHours = if (setting.needLowInsulinNoticeAmountSyncPatch) setting.lowInsulinNoticeAmount ?: 0 else null
        return SettingsSyncNeed(maxBolusDose, lowInsulinHours)
    }

    private fun initializeOnStart() {
        // Run initialization directly on start instead of gating it on EventAppInitialized.
        // onStart() is now a suspend function launched fire-and-forget, so the (non-replayed)
        // EventAppInitialized could fire before this subscription was registered — leaving the
        // patch uninitialized and appearing deactivated after an app update (dev fixed the same
        // race in eopatch). Parser registration, initPatchOnce() and reconnection do not depend on
        // other plugins; the basal profile is applied best-effort here and, if not yet available,
        // is set later via the framework's setNewBasalProfile().
        pluginDisposable += Completable.fromAction {
            carelevoProtocolParserRegister.registerParser()
        }
            .subscribeOn(aapsSchedulers.io)
            .doOnSubscribe { aapsLogger.debug(LTag.PUMPCOMM, "onStart: 1) parser registered start") }
            .doOnComplete { aapsLogger.debug(LTag.PUMPCOMM, "onStart: 1) parser registered done") }
            .andThen(
                carelevoPatch.initPatchOnce()
                    .timeout(5, TimeUnit.SECONDS)
                    .onErrorComplete()
                    .doOnSubscribe { aapsLogger.debug(LTag.PUMPCOMM, "onStart: 2) initPatchOnce waiting") }
                    .doOnComplete { aapsLogger.debug(LTag.PUMPCOMM, "onStart: 2) initPatchOnce completed") }
            )
            .andThen(
                Completable.fromAction {
                    val profile = runBlocking { profileFunction.getProfile() }
                    if (profile != null) {
                        carelevoPatch.setProfile(profile)
                        aapsLogger.debug(LTag.PUMPCOMM, "onStart: 3) setProfile done: $profile")
                    } else {
                        aapsLogger.debug(LTag.PUMPCOMM, "onStart: 3) profile not ready, deferring to setNewBasalProfile")
                    }
                }
            )
            .subscribe(
                { aapsLogger.debug(LTag.PUMPCOMM, "onStart: ALL COMPLETE") },
                { e -> aapsLogger.error(LTag.PUMPCOMM, "onStart: chain error", e) }
            )

        pluginDisposable += carelevoPatch.patchInfo
            .observeOn(aapsSchedulers.io)
            .subscribe({
                           _reservoirLevel.value = PumpInsulin(it.getOrNull()?.insulinRemain ?: 0.0)
                           _batteryLevel.value = 0
                       }, fabricPrivacy::logException)
    }

    private fun registerBleReceiverIfNeeded() {
        if (bleReceiverDisposable?.isDisposed == false) return

        bleReceiverDisposable = CarelevoObserveReceiver(context, createBluetoothIntentFilter())
            .subscribe { intent ->
                aapsLogger.debug(LTag.PUMPBTCOMM, "CarelevoObserveReceiver called: ${intent.action}")
                when (intent.action) {
                    BluetoothDevice.ACTION_BOND_STATE_CHANGED -> {
                        val bondState = intent.getIntExtra(
                            BluetoothDevice.EXTRA_BOND_STATE,
                            BluetoothDevice.ERROR
                        )
                        CarelevoBleSource.bluetoothState.value
                            ?.copy(isBonded = bondState.codeToBondingResult())
                            ?.let { CarelevoBleSource._bluetoothState.onNext(it) }
                    }

                    BluetoothAdapter.ACTION_STATE_CHANGED     -> {
                        val value = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                        if (value in setOf(BluetoothAdapter.STATE_ON, BluetoothAdapter.STATE_OFF, BluetoothAdapter.STATE_TURNING_ON, BluetoothAdapter.STATE_TURNING_OFF)) {
                            val isConnected = value == BluetoothAdapter.STATE_ON

                            CarelevoBleSource._bluetoothState.value?.copy(
                                isEnabled = value.codeToDeviceResult(),
                                isConnected = if (isConnected) {
                                    PeripheralConnectionState.CONN_STATE_NONE
                                } else {
                                    CarelevoBleSource._bluetoothState.value?.isConnected ?: PeripheralConnectionState.CONN_STATE_NONE
                                },
                            )?.let { CarelevoBleSource._bluetoothState.onNext(it) }
                        }
                    }

                    BluetoothDevice.ACTION_ACL_CONNECTED      -> Unit

                    BluetoothDevice.ACTION_ACL_DISCONNECTED   -> Unit
                }
            }

        bleReceiverDisposable?.let { pluginDisposable.add(it) }
    }

    private fun createBluetoothIntentFilter(): IntentFilter {
        return IntentFilter().apply {
            addAction(BluetoothDevice.ACTION_BOND_STATE_CHANGED)
            addAction(BluetoothAdapter.ACTION_STATE_CHANGED)
        }
    }

    private fun applyDefaultCageThresholdsIfNeeded() {
        if (sp.getBoolean(CarelevoBooleanPreferenceKey.CARELEVO_CAGE_DEFAULT_APPLIED.key, false)) return

        sp.edit {
            putInt(IntKey.OverviewCageWarning.key, 96)
            putInt(IntKey.OverviewCageCritical.key, 168)
            putBoolean(CarelevoBooleanPreferenceKey.CARELEVO_CAGE_DEFAULT_APPLIED.key, true)
        }
    }

    private suspend fun startAlarmObserving() {
        aapsLogger.debug(LTag.NOTIFICATION, "startAlarmObserving:: onStart")

        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_START) {
                aapsLogger.debug(LTag.NOTIFICATION, "Foreground transition -> refresh alarms")
                carelevoAlarmNotifier.refreshAlarms()
            }
        }
        lifecycleObserver = observer
        withContext(Dispatchers.Main) {
            ProcessLifecycleOwner.get().lifecycle.addObserver(observer)
        }

        carelevoAlarmNotifier.startObserving { alarms ->
            aapsLogger.debug(LTag.NOTIFICATION, "observe alarms size=${alarms.size}, $alarms")
            handleAlarms(alarms)
        }
    }

    private fun handleAlarms(alarms: List<CarelevoAlarmInfo>) {
        aapsLogger.debug(LTag.NOTIFICATION, "startAlarmObserving handleAlarms:: $alarms")
        if (alarms.isEmpty()) return

        if (
            alarms.any {
                it.alarmType.isCritical() ||
                    it.cause == AlarmCause.ALARM_ALERT_BLUETOOTH_OFF
            }
        ) {
            aapsLogger.debug(LTag.NOTIFICATION, "critical alarm handled by compose host")
        } else {
            carelevoAlarmNotifier.showTopNotification(alarms)
        }

    }

    override fun getPreferenceScreenContent() = PreferenceSubScreenDef(
        key = "carelevo_settings",
        titleResId = R.string.carelevo,
        items = listOf(
            CarelevoIntPreferenceKey.CARELEVO_LOW_INSULIN_EXPIRATION_REMINDER_HOURS.withEntries(
                (20..50 step 5).associateWith { "$it U" }
            ),
            CarelevoIntPreferenceKey.CARELEVO_PATCH_EXPIRATION_REMINDER_HOURS.withEntries(
                (24..167 step 1).associateWith { "$it ${rh.gs(app.aaps.core.interfaces.R.string.hours)}" }
            ),
            CarelevoBooleanPreferenceKey.CARELEVO_BUZZER_REMINDER,
            CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK
        ),
        icon = pluginDescription.icon
    )

    // A patch is "configured" once activation has persisted a patch record (patchInfo present) — the
    // same signal isInitialized() gates on first, so isInitialized() implies isConfigured() and the
    // contract invariant !isConfigured() => !isInitialized() holds by construction. Intentionally
    // independent of BLE: an attached-but-disconnected patch is still configured and may be delivering.
    override fun isConfigured(): Boolean =
        carelevoPatch.patchInfo.value?.getOrNull() != null

    override fun isInitialized(): Boolean {
        return connectionCoordinator.isInitialized()
    }

    override fun isSuspended(): Boolean {
        // Real delivery-suspend (pump stopped by the user), NOT the BLE connection state. Otherwise a
        // normal idle disconnect (NotConnectedBooted, now that disconnect() actually disconnects) would
        // be reported as suspended and surface as an error/suspended icon on the overview.
        return carelevoPatch.patchInfo.value?.getOrNull()?.isStopped ?: false
    }

    override fun isBusy(): Boolean {
        return false
    }

    override fun isConnected(): Boolean {
        return connectionCoordinator.isConnected()
    }

    override fun isConnecting(): Boolean {
        return false
    }

    override fun isHandshakeInProgress(): Boolean {
        return false
    }

    override fun connect(reason: String) {
        connectionCoordinator.connect(
            reason = reason,
            txUuid = txUuid,
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    override fun disconnect(reason: String) {
        connectionCoordinator.disconnect(reason)
    }

    override fun stopConnecting() {
        connectionCoordinator.stopConnecting()
    }

    override suspend fun getPumpStatus(reason: String) {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            readInfusionInfoViaNewStack()
            return
        }
        connectionCoordinator.refreshPumpStatus(
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    /**
     * Phase-2.B status read over the NEW [app.aaps.pump.carelevo.ble.BleClient] stack (flag-gated,
     * engineering-only): drop the legacy link, read Infusion Info (0x31 → 0x91) on the new transport's
     * own connection, and persist it through the SAME seam the legacy `_patchEvent` path uses
     * (`carelevoPatch.applyInfusionInfoReport`) — so this is a like-for-like replacement of the legacy
     * `refreshPumpStatus` (reservoir/pump-state/totals all refresh), not a throwaway diagnostic. Safe
     * against the CommandQueue: it runs on the QueueWorker thread while blocked inside this status read,
     * so the worker cannot concurrently re-dial legacy — it only reconnects legacy after this returns,
     * by which point the new session has closed. See `_docs/carelevo-new-ble-stack.md` (Phase 2).
     */
    private suspend fun readInfusionInfoViaNewStack() {
        val address = carelevoPatch.getPatchInfoAddress() ?: run {
            aapsLogger.warn(LTag.PUMPCOMM, "newBle.readInfusionInfo skipped: no patch address")
            return
        }
        try {
            // Own the link: drop the legacy GATT (and stop its reconnect) before the new transport
            // dials the same device, then let the stack release the old client interface.
            connectionCoordinator.disconnect("new-ble-session")
            delay(NEW_BLE_SETTLE_MS)
            val info = bleSession.readInfusionInfo(address)
            carelevoPatch.applyInfusionInfoReport(
                runningMinutes = info.runningMinutes,
                remains = info.insulinRemaining,
                infusedTotalBasalAmount = info.infusedTotalBasalAmount,
                infusedTotalBolusAmount = info.infusedTotalBolusAmount,
                pumpStateRaw = info.pumpStateRaw,
                modeRaw = info.modeRaw
            )
            _lastDataTime.value = System.currentTimeMillis()
            aapsLogger.info(
                LTag.PUMPCOMM,
                "newBle.readInfusionInfo OK remains=${info.insulinRemaining} basal=${info.infusedTotalBasalAmount} " +
                    "bolus=${info.infusedTotalBolusAmount} pumpState=${info.pumpStateRaw} mode=${info.modeRaw} running=${info.runningMinutes}"
            )
        } catch (e: Throwable) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.readInfusionInfo FAILED", e)
        }
    }

    override suspend fun setNewBasalProfile(profile: PumpProfile): PumpEnactResult {
        aapsLogger.debug(LTag.PUMP, "setNewBasalProfile called - ${carelevoPatch.resolvePatchState()}")
        _lastDataTime.value = System.currentTimeMillis()
        // PROFILE_SET_OK / FAILED_UPDATE_PROFILE are posted centrally by the CommandQueue from the returned
        // success/enacted (unified across pumps) — this method only returns the right values.
        val result = when (carelevoPatch.resolvePatchState()) {
            is PatchState.NotConnectedNotBooting -> {
                // No active patch yet — store the profile for when a patch is activated. A deferred write,
                // not an actual change, so enacted=false (no PROFILE_SET_OK); success=true keeps the
                // not-ready case out of the failure alarm (matches the other queue-managed pumps).
                carelevoPatch.setProfile(profile)
                pumpEnactResultProvider.get().success(true).enacted(false)
            }

            else -> {
                // Patch present. setNewBasalProfile runs on the queue worker AFTER the queue guaranteed a
                // fully-connected link, so this is the live push path even if the cached patchState briefly
                // reads NotConnectedBooted. updateBasalProfile returns the real success/enacted result.
                updateBasalProfile(profile)
            }
        }
        aapsLogger.debug(LTag.PUMP, "result success=${result.success} enacted=${result.enacted} comment=${result.comment}")
        return result
    }

    private fun updateBasalProfile(profile: Profile): PumpEnactResult {
        return basalProfileUpdateCoordinator.updateBasalProfile(
            profile = profile,
            cancelExtendedBolus = {
                bolusCoordinator.cancelExtendedBolus(
                    serialNumber = serialNumber(),
                    onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
                )
            },
            cancelTempBasal = {
                tempBasalCoordinator.cancelTempBasal(
                    serialNumber = serialNumber(),
                    onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
                )
            },
            onProfileUpdated = { updatedProfile ->
                _lastDataTime.value = System.currentTimeMillis()
                carelevoPatch.setProfile(updatedProfile)
            }
        )
    }

    override fun isThisProfileSet(profile: PumpProfile): Boolean {
        return carelevoPatch.checkIsSameProfile(profile)
    }

    // Activation ops (safety check, …) are queued so they get the CommandQueue's managed
    // connect-before-execute / reconnect lifecycle instead of a direct BLE call.
    override fun executeCustomCommand(customCommand: CustomCommand): PumpEnactResult? =
        activationExecutor.execute(customCommand)

    private val _lastDataTime = MutableStateFlow(0L)
    override val lastDataTime: StateFlow<Long> = _lastDataTime.asStateFlow()

    override val lastBolusTime: StateFlow<Long?>
        get() = bolusCoordinator.lastBolusTime

    override val lastBolusAmount: StateFlow<PumpInsulin?>
        get() = bolusCoordinator.lastBolusAmount

    override val baseBasalRate: PumpRate
        get() = PumpRate(carelevoPatch.profile.value?.getOrNull()?.getBasal() ?: 0.0)

    private val _reservoirLevel = MutableStateFlow(PumpInsulin(0.0))
    override val reservoirLevel: StateFlow<PumpInsulin> = _reservoirLevel

    private val _batteryLevel = MutableStateFlow<Int?>(null)
    override val batteryLevel: StateFlow<Int?> = _batteryLevel

    // start imme bolus infusion
    override suspend fun deliverTreatment(detailedBolusInfo: DetailedBolusInfo): PumpEnactResult {
        return bolusCoordinator.deliverTreatment(
            detailedBolusInfo = detailedBolusInfo,
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() },
            pluginDisposable = pluginDisposable
        )
    }

    // cancel imme bolus
    override fun stopBolusDelivering() {
        bolusCoordinator.cancelImmediateBolus(
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() },
            pluginDisposable = pluginDisposable
        )
    }

    override suspend fun setTempBasalAbsolute(absoluteRate: Double, durationInMinutes: Int, enforceNew: Boolean, tbrType: PumpSync.TemporaryBasalType): PumpEnactResult {
        return tempBasalCoordinator.setTempBasalAbsolute(
            absoluteRate = absoluteRate,
            durationInMinutes = durationInMinutes,
            tbrType = tbrType,
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    override suspend fun setTempBasalPercent(percent: Int, durationInMinutes: Int, enforceNew: Boolean, tbrType: PumpSync.TemporaryBasalType): PumpEnactResult {
        return tempBasalCoordinator.setTempBasalPercent(
            percent = percent,
            durationInMinutes = durationInMinutes,
            tbrType = tbrType,
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    override suspend fun cancelTempBasal(enforceNew: Boolean): PumpEnactResult {
        return tempBasalCoordinator.cancelTempBasal(
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    override suspend fun setExtendedBolus(insulin: Double, durationInMinutes: Int): PumpEnactResult {
        return bolusCoordinator.setExtendedBolus(
            insulin = insulin,
            durationInMinutes = durationInMinutes,
            serialNumber = serialNumber()
        )
    }

    override suspend fun cancelExtendedBolus(): PumpEnactResult {
        return bolusCoordinator.cancelExtendedBolus(
            serialNumber = serialNumber(),
            onLastDataUpdated = { _lastDataTime.value = System.currentTimeMillis() }
        )
    }

    override fun manufacturer(): ManufacturerType {
        return ManufacturerType.CareMedi
    }

    override fun model(): PumpType {
        return PumpType.CAREMEDI_CARELEVO
    }

    override fun serialNumber(): String {
        return carelevoPatch.patchInfo.value?.getOrNull()?.manufactureNumber ?: ""
    }

    override val pumpDescription: PumpDescription
        get() = _pumpDescription

    override val isFakingTempsByExtendedBoluses: Boolean
        get() = false

    override suspend fun loadTDDs(): PumpEnactResult {
        return pumpEnactResultProvider.get()
    }

    override fun canHandleDST(): Boolean {
        return false
    }

    override suspend fun timezoneOrDSTChanged(timeChangeType: TimeChangeType) {
        super.timezoneOrDSTChanged(timeChangeType)
        // Route through the queue (connect-before-execute) like every other patch op. The pump
        // idle-disconnects between commands, so a direct fire-and-forget write would silently fail
        // while resting. Skip ONLY when no patch is active; if the patch is present but its insulinRemain
        // is not yet known (e.g. before the first status read after reconnect) still push with 0 rather
        // than dropping the clock update (original `?: 0` semantics).
        val patchInfo = carelevoPatch.patchInfo.value?.getOrNull() ?: return
        val insulin = patchInfo.insulinRemain?.toInt() ?: 0
        val result = commandQueue.customCommand(CmdTimeZoneUpdate(insulinAmount = insulin))
        if (result.success) _lastDataTime.value = System.currentTimeMillis()
    }
}
