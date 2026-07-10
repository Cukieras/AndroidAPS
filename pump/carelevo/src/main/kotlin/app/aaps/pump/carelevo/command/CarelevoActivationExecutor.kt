package app.aaps.pump.carelevo.command

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.PumpEnactResult
import app.aaps.core.interfaces.queue.CustomCommand
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.patch.NeedleCheckSuccess
import app.aaps.pump.carelevo.domain.type.SafetyProgress
import app.aaps.pump.carelevo.domain.usecase.basal.CarelevoSetBasalProgramUseCase
import app.aaps.pump.carelevo.domain.usecase.basal.model.SetBasalProgramRequestModel
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchAdditionalPrimingUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchNeedleInsertionCheckUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchSafetyCheckUseCase
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
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
    private val pumpEnactResultProvider: Provider<PumpEnactResult>,
    private val carelevoPatch: CarelevoPatch,
    private val safetyCheckUseCase: CarelevoPatchSafetyCheckUseCase,
    private val needleCheckUseCase: CarelevoPatchNeedleInsertionCheckUseCase,
    private val setBasalUseCase: CarelevoSetBasalProgramUseCase,
    private val additionalPrimingUseCase: CarelevoPatchAdditionalPrimingUseCase,
    private val discardUseCase: CarelevoPatchDiscardUseCase
) {

    private companion object {

        // Upper bounds so a hung use case can never block the queue worker thread forever. These
        // mirror the timeouts the ViewModels used before the ops were routed through the queue.
        private const val NEEDLE_CHECK_TIMEOUT_SEC = 30L
        private const val SET_BASAL_TIMEOUT_SEC = 15L
        private const val ADDITIONAL_PRIMING_TIMEOUT_SEC = 60L
        private const val DISCARD_TIMEOUT_SEC = 30L
    }

    private val _safetyProgress = MutableSharedFlow<SafetyProgress>(extraBufferCapacity = 16)
    val safetyProgress: SharedFlow<SafetyProgress> = _safetyProgress.asSharedFlow()

    fun execute(command: CustomCommand): PumpEnactResult? = when (command) {
        is CmdSafetyCheck       -> runSafetyCheck()
        is CmdNeedleCheck       -> runNeedleCheck()
        is CmdSetBasal          -> runSetBasal()
        is CmdAdditionalPriming -> runAdditionalPriming()
        is CmdDiscard           -> runDiscard()
        else                    -> null
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
            val response = needleCheckUseCase.execute()
                .timeout(NEEDLE_CHECK_TIMEOUT_SEC, TimeUnit.SECONDS)
                .blockingGet()
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
            val response = setBasalUseCase.execute(SetBasalProgramRequestModel(profile))
                .timeout(SET_BASAL_TIMEOUT_SEC, TimeUnit.SECONDS)
                .blockingGet()
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
            val response = additionalPrimingUseCase.execute()
                .timeout(ADDITIONAL_PRIMING_TIMEOUT_SEC, TimeUnit.SECONDS)
                .blockingGet()
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdAdditionalPriming exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }

    /**
     * BLE discard (tell the patch to stop delivering). Routed through the queue so a dropped link is
     * reconnected before the stop is sent. The DB-only force-discard is left in the ViewModels as the
     * fallback for when the queue cannot reach the patch at all.
     */
    private fun runDiscard(): PumpEnactResult {
        val result = pumpEnactResultProvider.get()
        return try {
            val response = discardUseCase.execute()
                .timeout(DISCARD_TIMEOUT_SEC, TimeUnit.SECONDS)
                .blockingGet()
            val success = response is ResponseResult.Success
            result.success(success).enacted(success)
        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPCOMM, "CmdDiscard exception", e)
            result.success(false).enacted(false).comment(e.message ?: "error")
        }
    }
}
