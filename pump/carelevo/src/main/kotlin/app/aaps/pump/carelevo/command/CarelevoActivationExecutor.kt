package app.aaps.pump.carelevo.command

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.queue.CustomCommand
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.pump.carelevo.ble.CarelevoBleSession
import app.aaps.pump.carelevo.ble.commands.BuzzModeCommand
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.keys.CarelevoBooleanPreferenceKey
import app.aaps.pump.carelevo.coordinator.CarelevoConnectionCoordinator
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.patch.NeedleCheckSuccess
import app.aaps.pump.carelevo.domain.type.SafetyProgress
import app.aaps.pump.carelevo.domain.usecase.alarm.AlarmClearPatchDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.alarm.AlarmClearRequestUseCase
import app.aaps.pump.carelevo.domain.usecase.alarm.model.AlarmClearUseCaseRequest
import app.aaps.pump.carelevo.domain.usecase.basal.CarelevoSetBasalProgramUseCase
import app.aaps.pump.carelevo.domain.usecase.basal.model.SetBasalProgramRequestModel
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpResumeUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpStopUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.model.CarelevoPumpStopRequestModel
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchAdditionalPrimingUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchNeedleInsertionCheckUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchSafetyCheckUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchTimeZoneUpdateUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.model.CarelevoPatchTimeZoneRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoPatchBuzzModifyUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoPatchExpiredThresholdModifyUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoUpdateLowInsulinNoticeAmountUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.CarelevoUpdateMaxBolusDoseUseCase
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoPatchBuzzRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoPatchExpiredThresholdModifyRequestModel
import app.aaps.pump.carelevo.domain.usecase.userSetting.model.CarelevoUserSettingInfoRequestModel
import io.reactivex.rxjava3.core.Single
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.runBlocking
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Provider
import javax.inject.Singleton
import kotlin.jvm.optionals.getOrNull

/**
 * Runs activation operations that are routed through the AAPS [app.aaps.core.interfaces.queue.CommandQueue]
 * as custom commands. The plugin's `executeCustomCommand` delegates here; it is invoked on the queue
 * worker thread AFTER the queue has guaranteed the pump is connected (connect-before-execute + managed
 * reconnect). Each op therefore runs BLOCKING and returns a [PumpEnactResult].
 *
 * The safety check streams progress (`Progress` → `Success`/`Error`); since a custom command returns a
 * single result, that progress is republished on [safetyProgress] so the wizard can drive its countdown.
 */
@Singleton
class CarelevoActivationExecutor @Inject constructor(
    private val aapsLogger: AAPSLogger,
    private val aapsSchedulers: AapsSchedulers,
    private val pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoPatch: CarelevoPatch,
    private val safetyCheckUseCase: CarelevoPatchSafetyCheckUseCase,
    private val needleCheckUseCase: CarelevoPatchNeedleInsertionCheckUseCase,
    private val setBasalUseCase: CarelevoSetBasalProgramUseCase,
    private val additionalPrimingUseCase: CarelevoPatchAdditionalPrimingUseCase,
    private val discardUseCase: CarelevoPatchDiscardUseCase,
    private val pumpStopUseCase: CarelevoPumpStopUseCase,
    private val pumpResumeUseCase: CarelevoPumpResumeUseCase,
    private val timeZoneUpdateUseCase: CarelevoPatchTimeZoneUpdateUseCase,
    private val updateMaxBolusDoseUseCase: CarelevoUpdateMaxBolusDoseUseCase,
    private val updateLowInsulinNoticeAmountUseCase: CarelevoUpdateLowInsulinNoticeAmountUseCase,
    private val expiredThresholdModifyUseCase: CarelevoPatchExpiredThresholdModifyUseCase,
    private val buzzModifyUseCase: CarelevoPatchBuzzModifyUseCase,
    private val alarmClearRequestUseCase: AlarmClearRequestUseCase,
    private val alarmClearPatchDiscardUseCase: AlarmClearPatchDiscardUseCase,
    // Phase-2 new BLE stack (flag-gated). See `_docs/carelevo-new-ble-stack.md`.
    private val preferences: Preferences,
    private val bleSession: CarelevoBleSession,
    private val connectionCoordinator: CarelevoConnectionCoordinator
) {

    private companion object {

        // Upper bounds so a hung use case can never block the queue worker thread forever. These
        // mirror the timeouts the ViewModels used before the ops were routed through the queue.
        private const val NEEDLE_CHECK_TIMEOUT_SEC = 30L
        private const val SET_BASAL_TIMEOUT_SEC = 15L
        private const val ADDITIONAL_PRIMING_TIMEOUT_SEC = 60L
        private const val DISCARD_TIMEOUT_SEC = 30L
        private const val PUMP_STOP_TIMEOUT_SEC = 15L
        private const val PUMP_RESUME_TIMEOUT_SEC = 15L
        private const val TIMEZONE_UPDATE_TIMEOUT_SEC = 15L
        private const val SETTINGS_TIMEOUT_SEC = 15L

        // The alarm use cases already wait up to 10s internally for the patch's clear/discard event.
        private const val ALARM_TIMEOUT_SEC = 20L

        // Settle window after dropping the legacy GATT before the new transport re-dials the same
        // device (mirrors CarelevoPumpPlugin). Phase-2 new-stack path only.
        private const val NEW_BLE_SETTLE_MS = 1000L
        private const val RESULT_SUCCESS = 0 // pump result byte 0 = SUCCESS (legacy Result taxonomy)
    }

    private val _safetyProgress = MutableSharedFlow<SafetyProgress>(extraBufferCapacity = 16)
    val safetyProgress: SharedFlow<SafetyProgress> = _safetyProgress.asSharedFlow()

    // Run a use-case Single on IO — NOT inline on the queue worker thread — with a hard timeout. The patch
    // use cases are `Single.fromCallable { ...blockingFirst()... }` with NO subscribeOn, so without moving
    // the subscription off the worker the inline block would ignore .timeout() and hang the single
    // CommandQueue thread forever whenever a patch confirmation event is lost / the link drops mid-wait.
    private fun <T : Any> awaitOnIo(single: Single<T>, timeoutSec: Long): T =
        single.subscribeOn(aapsSchedulers.io).timeout(timeoutSec, TimeUnit.SECONDS).blockingGet()

    fun execute(command: CustomCommand): PumpEnactResult? = when (command) {
        is CmdSafetyCheck            -> runSafetyCheck()
        is CmdNeedleCheck            -> runNeedleCheck()
        is CmdSetBasal               -> runSetBasal()
        is CmdAdditionalPriming      -> runAdditionalPriming()
        is CmdDiscard                -> runDiscard()
        is CmdPumpStop               -> runPumpStop(command)
        is CmdPumpResume             -> runPumpResume()
        is CmdTimeZoneUpdate         -> runTimeZoneUpdate(command)
        is CmdUpdateMaxBolus         -> runUpdateMaxBolus(command)
        is CmdUpdateLowInsulinNotice -> runUpdateLowInsulinNotice(command)
        is CmdUpdateExpiredThreshold -> runUpdateExpiredThreshold(command)
        is CmdUpdateBuzzer           -> runUpdateBuzzer(command)
        is CmdAlarmClear             -> runAlarmClear(command)
        is CmdAlarmClearPatchDiscard -> runAlarmClearPatchDiscard(command)
        else                         -> null
    }

    private fun runSafetyCheck(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        var success = false
        try {
            // Blocks the queue worker thread until the check reaches a terminal state; the use case
            // has its own internal timeouts so it always completes. Progress is mirrored to the UI.
            safetyCheckUseCase.execute()
                .blockingSubscribe(
                    { progress ->
                        _safetyProgress.tryEmit(progress)
                        if (progress is SafetyProgress.Success) success = true
                    },
                    { error ->
                        aapsLogger.error(LTag.PUMPCOMM, "CmdSafetyCheck error", error)
                        _safetyProgress.tryEmit(SafetyProgress.Error(error))
                    }
                )
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdSafetyCheck exception", e)
            _safetyProgress.tryEmit(SafetyProgress.Error(e))
            return result.success(false).enacted(false).comment(e.message ?: "error")
        }
        return result.success(success).enacted(success)
    }

    private fun runNeedleCheck(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(needleCheckUseCase.execute(), NEEDLE_CHECK_TIMEOUT_SEC)
            val success = response is ResponseResult.Success && response.data is NeedleCheckSuccess
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdNeedleCheck exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runSetBasal(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val profile = carelevoPatch.profile.value?.getOrNull()
            ?: return result.success(false).enacted(false).comment("profile not set")
        return try {
            val response = awaitOnIo(setBasalUseCase.execute(SetBasalProgramRequestModel(profile)), SET_BASAL_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdSetBasal exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAdditionalPriming(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(additionalPrimingUseCase.execute(), ADDITIONAL_PRIMING_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAdditionalPriming exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * BLE discard (tell the patch to stop delivering) + teardown. Routed through the queue so a
     * dropped link is reconnected before the stop is sent, and so the teardown runs HERE on the queue
     * worker thread: the worker is busy inside this command while it runs, so it cannot fire a
     * reconnect at the patch we are discarding (no zombie-reconnect race), and the ~300ms BLE teardown
     * stays off the Main thread. The DB-only force-discard is left in the ViewModels as the fallback
     * for when the queue cannot reach the patch at all.
     */
    private fun runDiscard(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        // The STOP alone decides success: a teardown failure only leaves a stale bond and must NOT
        // flip an already-successful stop to failure. Teardown runs on this (queue worker) thread so
        // the worker cannot fire a reconnect during it, and is single-flight inside discardTeardown().
        val stopped = try {
            awaitOnIo(discardUseCase.execute(), DISCARD_TIMEOUT_SEC) is ResponseResult.Success
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdDiscard stop exception", e)
            false
        }
        if (stopped) {
            carelevoPatch.discardTeardown()
        }
        return result.success(stopped).enacted(stopped)
    }

    private fun runPumpStop(command: CmdPumpStop): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(pumpStopUseCase.execute(CarelevoPumpStopRequestModel(durationMin = command.durationMin)), PUMP_STOP_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdPumpStop exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runPumpResume(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(pumpResumeUseCase.execute(), PUMP_RESUME_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdPumpResume exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runTimeZoneUpdate(command: CmdTimeZoneUpdate): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(timeZoneUpdateUseCase.execute(CarelevoPatchTimeZoneRequestModel(insulinAmount = command.insulinAmount)), TIMEZONE_UPDATE_TIMEOUT_SEC)
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdTimeZoneUpdate exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    // patchState is read HERE (queue worker thread, after the reconnect) so the setting is pushed with the
    // patch's current state, not whatever it was when the preference-change enqueued the command.
    private fun runUpdateMaxBolus(command: CmdUpdateMaxBolus): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                updateMaxBolusDoseUseCase.execute(
                    CarelevoUserSettingInfoRequestModel(patchState = patchState, maxBolusDose = command.maxBolusDose)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateMaxBolus exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateLowInsulinNotice(command: CmdUpdateLowInsulinNotice): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                updateLowInsulinNoticeAmountUseCase.execute(
                    CarelevoUserSettingInfoRequestModel(patchState = patchState, lowInsulinNoticeAmount = command.hours)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateLowInsulinNotice exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateExpiredThreshold(command: CmdUpdateExpiredThreshold): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                expiredThresholdModifyUseCase.execute(
                    CarelevoPatchExpiredThresholdModifyRequestModel(patchState = patchState, patchExpiredThreshold = command.hours)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateExpiredThreshold exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runUpdateBuzzer(command: CmdUpdateBuzzer): PumpEnactResult {
        if (preferences.get(CarelevoBooleanPreferenceKey.CARELEVO_USE_NEW_BLE_STACK)) {
            return runBuzzerViaNewStack(command.on)
        }
        val result = pumpEnactResultProvider.get()
        val patchState = carelevoPatch.patchState.value?.getOrNull()
        return try {
            val response = awaitOnIo(
                buzzModifyUseCase.execute(
                    CarelevoPatchBuzzRequestModel(patchState = patchState, settingsAlarmBuzz = command.on)
                ),
                SETTINGS_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdUpdateBuzzer exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * Phase-2 first WRITE over the new [app.aaps.pump.carelevo.ble.BleClient] stack (flag-gated,
     * engineering-only): set the patch buzzer via [BuzzModeCommand] (0x18 → 0x78). Chosen as the first
     * write to device-validate because it is non-therapy and audibly verifiable (the patch buzzes). Same
     * connection-ownership handling as the plugin's status read: drop the legacy link + settle, then run
     * the command on the new transport's own session (this runs on the queue worker thread, blocked
     * inside the command, so the worker cannot concurrently re-dial legacy). See `_docs/carelevo-new-ble-stack.md`.
     */
    private fun runBuzzerViaNewStack(on: Boolean): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        val address = carelevoPatch.getPatchInfoAddress()
            ?: return result.success(false).enacted(false).comment("no patch address")
        return try {
            connectionCoordinator.disconnect("new-ble-session")
            val response = runBlocking {
                delay(NEW_BLE_SETTLE_MS)
                bleSession.runSingle(address, BuzzModeCommand(use = on))
            }
            val success = response.resultCode == RESULT_SUCCESS
            aapsLogger.info(LTag.PUMPCOMM, "newBle.buzzer OK on=$on result=${response.resultCode}")
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "newBle.buzzer FAILED", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAlarmClear(command: CmdAlarmClear): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(
                alarmClearRequestUseCase.execute(
                    AlarmClearUseCaseRequest(alarmId = command.alarmId, alarmType = command.alarmType, alarmCause = command.alarmCause)
                ),
                ALARM_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAlarmClear exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    private fun runAlarmClearPatchDiscard(command: CmdAlarmClearPatchDiscard): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = awaitOnIo(
                alarmClearPatchDiscardUseCase.execute(
                    AlarmClearUseCaseRequest(alarmId = command.alarmId, alarmType = command.alarmType, alarmCause = command.alarmCause)
                ),
                ALARM_TIMEOUT_SEC
            )
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAlarmClearPatchDiscard exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }
}
