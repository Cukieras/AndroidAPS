package app.aaps.pump.carelevo.presentation.viewmodel

import android.content.Context
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.SwapHoriz
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import app.aaps.core.data.pump.defs.PumpType
import app.aaps.core.data.time.T
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.pump.PumpRate
import app.aaps.core.interfaces.pump.PumpSync
import app.aaps.core.interfaces.queue.CommandQueue
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.utils.DateUtil
import app.aaps.core.ui.R as CoreUiR
import app.aaps.core.ui.compose.StatusLevel
import app.aaps.core.ui.compose.icons.IcLoopPaused
import app.aaps.core.ui.compose.pump.ActionCategory
import app.aaps.core.ui.compose.pump.PumpAction
import app.aaps.core.ui.compose.pump.PumpCommunicationStatus
import app.aaps.core.ui.compose.pump.PumpInfoRow
import app.aaps.core.ui.compose.pump.PumpOverviewUiState
import app.aaps.core.ui.compose.pump.StatusBanner
import app.aaps.core.ui.compose.pump.tickerFlow
import app.aaps.pump.carelevo.R
import app.aaps.pump.carelevo.ble.core.CarelevoBleController
import app.aaps.pump.carelevo.ble.data.DeviceModuleState
import app.aaps.pump.carelevo.command.CmdDiscard
import app.aaps.pump.carelevo.common.CarelevoPatch
import app.aaps.pump.carelevo.common.MutableEventFlow
import app.aaps.pump.carelevo.common.asEventFlow
import app.aaps.pump.carelevo.common.model.Event
import app.aaps.pump.carelevo.common.model.PatchState
import app.aaps.pump.carelevo.common.model.State
import app.aaps.pump.carelevo.common.model.UiState
import app.aaps.pump.carelevo.domain.model.ResponseResult
import app.aaps.pump.carelevo.domain.model.infusion.CarelevoInfusionInfoDomainModel
import app.aaps.pump.carelevo.domain.model.patch.CarelevoPatchInfoDomainModel
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoDeleteInfusionInfoUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpResumeUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.CarelevoPumpStopUseCase
import app.aaps.pump.carelevo.domain.usecase.infusion.model.CarelevoDeleteInfusionRequestModel
import app.aaps.pump.carelevo.domain.usecase.infusion.model.CarelevoPumpStopRequestModel
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoPatchForceDiscardUseCase
import app.aaps.pump.carelevo.domain.usecase.patch.CarelevoRequestPatchInfusionInfoUseCase
import app.aaps.pump.carelevo.presentation.model.CarelevoOverviewEvent
import app.aaps.pump.carelevo.presentation.model.CarelevoOverviewUiModel
import app.aaps.pump.carelevo.presentation.type.CarelevoScreenType
import dagger.hilt.android.lifecycle.HiltViewModel
import dagger.hilt.android.qualifiers.ApplicationContext
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.joda.time.DateTime
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Locale
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import kotlin.jvm.optionals.getOrNull

@HiltViewModel
class CarelevoOverviewViewModel @Inject constructor(
    private val rh: ResourceHelper,
    private val pumpSync: PumpSync,
    private val dateUtil: DateUtil,
    private val commandQueue: CommandQueue,
    private val aapsLogger: AAPSLogger,
    private val carelevoPatch: CarelevoPatch,
    private val bleController: CarelevoBleController,
    private val aapsSchedulers: AapsSchedulers,
    private val patchForceDiscardUseCase: CarelevoPatchForceDiscardUseCase,
    private val pumpStopUseCase: CarelevoPumpStopUseCase,
    private val pumpResumeUseCase: CarelevoPumpResumeUseCase,
    private val requestPatchInfusionInfoUseCase: CarelevoRequestPatchInfusionInfoUseCase,
    private val carelevoDeleteInfusionInfoUseCase: CarelevoDeleteInfusionInfoUseCase,
    private val rxBus: RxBus,
    @ApplicationContext private val context: Context
) : ViewModel() {

    private val _bluetoothState = MutableLiveData<DeviceModuleState?>()
    val bluetoothState: LiveData<DeviceModuleState?> get() = _bluetoothState

    private val _patchState = MutableLiveData<PatchState>(PatchState.NotConnectedNotBooting)
    val patchState: LiveData<PatchState?> get() = _patchState

    private val _serialNumber = MutableLiveData<String>()
    val serialNumber get() = _serialNumber

    private val _lotNumber = MutableLiveData<String>()
    val lotNumber get() = _lotNumber

    private val _bootDateTime = MutableLiveData<String>()
    val bootDateTime get() = _bootDateTime

    private val _expirationTime = MutableLiveData<String>()
    val expirationTime get() = _expirationTime

    private val _basalRate = MutableLiveData<Double>()
    val basalRate get() = _basalRate

    private val _tempBasalRate = MutableLiveData<Double?>()
    val tempBasalRate get() = _tempBasalRate

    private val _insulinRemains = MutableLiveData<String?>()
    val insulinRemains get() = _insulinRemains

    private val _totalInsulinAmount = MutableLiveData<Double?>()
    val totalInsulinAmount get() = _totalInsulinAmount

    private val _runningRemainMinutes = MutableLiveData<Int?>()
    val runningRemainMinutes get() = _runningRemainMinutes

    private var _isCreated = false
    val isCreated get() = _isCreated

    private val _event = MutableEventFlow<Event>()
    val event = _event.asEventFlow()

    private val _uiState: MutableStateFlow<State> = MutableStateFlow(UiState.Idle)
    val uiState = _uiState.asStateFlow()

    private val _bluetoothStateFlow = MutableStateFlow<DeviceModuleState?>(null)
    private val _patchStateFlow = MutableStateFlow<PatchState>(PatchState.NotConnectedNotBooting)
    private val _overviewDataFlow = MutableStateFlow(defaultOverviewData())
    private val _basalRateFlow = MutableStateFlow(0.0)
    private val _tempBasalRateFlow = MutableStateFlow<Double?>(null)

    private val communicationStatus = PumpCommunicationStatus(rxBus, commandQueue, context, viewModelScope)

    private val overviewInputs = combine(
        _bluetoothStateFlow,
        _patchStateFlow,
        _overviewDataFlow,
        _basalRateFlow,
        _tempBasalRateFlow
    ) { bluetoothState, patchState, overviewData, basalRate, tempBasalRate ->
        OverviewInputs(
            bluetoothState = bluetoothState,
            patchState = patchState,
            overviewData = overviewData,
            basalRate = basalRate,
            tempBasalRate = tempBasalRate
        )
    }

    val overviewUiState = combine(
        overviewInputs,
        communicationStatus.refreshTrigger,
        tickerFlow(30_000L)
    ) { inputs, _, _ ->
        buildOverviewState(
            bluetoothState = inputs.bluetoothState,
            patchState = inputs.patchState,
            overviewData = inputs.overviewData,
            basalRate = inputs.basalRate,
            tempBasalRate = inputs.tempBasalRate
        )
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000L),
        initialValue = buildOverviewState(
            bluetoothState = _bluetoothStateFlow.value,
            patchState = _patchStateFlow.value,
            overviewData = _overviewDataFlow.value,
            basalRate = _basalRateFlow.value,
            tempBasalRate = _tempBasalRateFlow.value
        )
    )

    private var _isPumpStop = MutableLiveData(false)
    val isPumpStop get() = _isPumpStop

    private var _isCheckScreen = MutableStateFlow<CarelevoScreenType?>(null)
    val isCheckScreen get() = _isCheckScreen

    private val _hasUnacknowledgedAlarms = MutableStateFlow(false)
    val hasUnacknowledgedAlarms = _hasUnacknowledgedAlarms.asStateFlow()

    private val compositeDisposable = CompositeDisposable()

    val secondTick: Flow<DateTime> = flow {
        while (currentCoroutineContext().isActive) {
            val now = DateTime.now()
            emit(now)
            delay((1000 - now.millisOfSecond).coerceIn(1, 1000).toLong())
        }
    }.flowOn(Dispatchers.Default)

    init {
        viewModelScope.launch {
            secondTick.collect {
                clearExpiredInfusions()
            }
        }
    }

    fun setIsCreated(isCreated: Boolean) {
        _isCreated = isCreated
    }

    fun observeBleState() {
        compositeDisposable += carelevoPatch.btState
            .observeOn(aapsSchedulers.main)
            .subscribe { btState ->
                val btState = btState.getOrNull() ?: return@subscribe
                aapsLogger.debug("[observeBleState] btState: ${btState.isEnabled}")
                _bluetoothState.value = btState.isEnabled
                _bluetoothStateFlow.value = btState.isEnabled
            }
    }

    fun observePatchInfo() {
        compositeDisposable += carelevoPatch.patchInfo
            .observeOn(aapsSchedulers.io)
            .flatMap { info ->
                val patchInfo = info?.getOrNull()
                if (patchInfo == null) {
                    aapsLogger.debug(LTag.PUMPCOMM, "[observePatchInfo] skip null/failure")
                    _isCheckScreen.tryEmit(null)
                    Observable.empty()
                } else {
                    aapsLogger.debug(LTag.PUMPCOMM, "[observePatchInfo] state: $patchInfo")
                    updateCheckScreen(patchInfo)
                    Observable.just(buildUi(patchInfo))
                }
            }
            .observeOn(aapsSchedulers.main)
            .doOnNext { ui -> updateState(ui) }
            .subscribe(
                { ui ->
                    aapsLogger.debug(LTag.PUMPCOMM, "state : $ui")
                },
                { e ->
                    aapsLogger.debug(LTag.PUMPCOMM, "onError", e)
                }
            )
    }

    private fun updateCheckScreen(patchInfo: CarelevoPatchInfoDomainModel) {
        val screenType = when {
            patchInfo.checkNeedle == false                         -> {
                val count = patchInfo.needleFailedCount
                if (count != null && count < 3) CarelevoScreenType.NEEDLE_INSERTION else null
            }

            patchInfo.checkSafety == null                          -> CarelevoScreenType.SAFETY_CHECK
            patchInfo.checkSafety && patchInfo.checkNeedle == null -> CarelevoScreenType.SAFETY_CHECK
            else                                                   -> null
        }
        _isCheckScreen.tryEmit(screenType)
    }

    private fun updateState(ui: CarelevoOverviewUiModel) {
        _overviewDataFlow.value = ui
        _serialNumber.value = ui.serialNumber
        _lotNumber.value = ui.lotNumber
        _bootDateTime.value = ui.bootDateTimeUi
        _expirationTime.value = ui.expirationTime
        //_infusionStatus.value = ui.infusionStatus
        _insulinRemains.value = ui.insulinRemainText
        _totalInsulinAmount.value = String.format(Locale.US, "%.2f", ui.totalBasal + ui.totalBolus).toDouble()
        _isPumpStop.value = ui.isPumpStopped
        _runningRemainMinutes.value = ui.runningRemainMinutes
    }

    private fun buildUi(info: CarelevoPatchInfoDomainModel): CarelevoOverviewUiModel {
        aapsLogger.debug(LTag.PUMPCOMM, "info : $info")
        val bootLdt = parseBootDateTime(info.bootDateTimeUtcMillis) ?: parseBootDateTime(info.bootDateTime)
        val bootUi = bootLdt?.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")) ?: ""

        val infusedBasal = (info.infusedTotalBasalAmount ?: 0.0)
            .toBigDecimal().setScale(2, RoundingMode.HALF_UP).toDouble()
        val infusedBolus = (info.infusedTotalBolusAmount ?: 0.0)
            .toBigDecimal().setScale(2, RoundingMode.HALF_UP).toDouble()

        val remainMinutes = bootLdt?.let { getRemainMin(it) } ?: 0
        val expireAt = bootLdt?.let { getExpireAtText(it) } ?: ""

        return CarelevoOverviewUiModel(
            serialNumber = info.manufactureNumber.orEmpty(),
            lotNumber = info.firmwareVersion.orEmpty(),
            bootDateTimeUi = bootUi,
            expirationTime = expireAt,
            infusionStatus = info.mode,
            insulinRemainText = if (info.insulinRemain != null && info.insulinAmount != null)
                rh.gs(
                    R.string.carelevo_insulin_remain_value,
                    rh.gs(R.string.common_label_unit_value_dose_with_space, info.insulinRemain),
                    rh.gs(R.string.common_label_unit_value_dose_with_space, info.insulinAmount)
                )
            else "",
            totalBasal = infusedBasal,
            totalBolus = infusedBolus,
            isPumpStopped = info.isStopped ?: false,
            runningRemainMinutes = remainMinutes
        )
    }

    fun observePatchState() {
        compositeDisposable += carelevoPatch.patchState
            .observeOn(aapsSchedulers.main)
            .subscribe(
                { response ->
                    aapsLogger.debug(LTag.PUMPCOMM, "state : ${response.getOrNull()}")
                    response?.getOrNull()?.let { patchState ->
                        _patchState.value = patchState
                        _patchStateFlow.value = patchState
                        if (patchState == PatchState.NotConnectedNotBooting) {
                            onDisconnectValue()
                        } else {
                            val basalRate = carelevoPatch.profile.value?.getOrNull()?.getBasal() ?: 0.0
                            _basalRate.value = basalRate
                            _basalRateFlow.value = basalRate
                        }
                    }
                },
                {
                    aapsLogger.debug(LTag.PUMPCOMM, "doOnError called : $it")
                }
            )
    }

    fun observeInfusionInfo() {
        compositeDisposable += carelevoPatch.infusionInfo
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.main)
            .subscribe {
                val infusionInfo = it.getOrNull() ?: run {
                    val patchInfo = carelevoPatch.patchInfo.value?.getOrNull() ?: return@subscribe
                    if (patchInfo.checkNeedle == true) {
                        _isCheckScreen.tryEmit(CarelevoScreenType.NEEDLE_INSERTION)
                    }
                    return@subscribe
                }
                handleInfusionProgram(infusionInfo)
            }
    }

    private fun handleInfusionProgram(info: CarelevoInfusionInfoDomainModel) {
        val temp = info.tempBasalInfusionInfo
        _tempBasalRate.value = temp?.speed
        _tempBasalRateFlow.value = temp?.speed
    }

    private fun clearExpiredInfusions() {
        val infusionInfo = carelevoPatch.infusionInfo.value?.getOrNull() ?: return
        val tempBasalInfusionInfo = infusionInfo.tempBasalInfusionInfo
        val immeBolusInfusionInfo = infusionInfo.immeBolusInfusionInfo
        val extendBolusInfusionInfo = infusionInfo.extendBolusInfusionInfo

        val now = DateTime.now()

        val tempBasal = tempBasalInfusionInfo?.takeIf { infusion ->
            val duration = infusion.infusionDurationMin ?: return@takeIf true
            val endTime = infusion.createdAt.plusMinutes(duration)
            endTime.isAfter(now)
        }

        val immeBolus = immeBolusInfusionInfo?.takeIf { infusion ->
            val duration = infusion.infusionDurationSeconds ?: return@takeIf true
            val endTime = infusion.createdAt.plusSeconds(duration)
            endTime.isAfter(now)
        }

        val extendBolus = extendBolusInfusionInfo?.takeIf { infusion ->
            val duration = infusion.infusionDurationMin ?: return@takeIf true
            val endTime = infusion.createdAt.plusMinutes(duration)
            endTime.isAfter(now)
        }

        val deleteTemp = (infusionInfo.tempBasalInfusionInfo != null && tempBasal == null)
        val deleteImme = (infusionInfo.immeBolusInfusionInfo != null && immeBolus == null)
        val deleteExtend = (infusionInfo.extendBolusInfusionInfo != null && extendBolus == null)

        if (!deleteTemp && !deleteImme && !deleteExtend) return

        val requestModel = CarelevoDeleteInfusionRequestModel(
            isDeleteTempBasal = deleteTemp,
            isDeleteImmeBolus = deleteImme,
            isDeleteExtendBolus = deleteExtend
        )
        clearInfusionInfo(requestModel)
    }

    fun clearInfusionInfo(requestModel: CarelevoDeleteInfusionRequestModel) {
        compositeDisposable += carelevoDeleteInfusionInfoUseCase.execute(requestModel)
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.main)
            .subscribe(
                { optionalList ->
                    aapsLogger.debug(LTag.PUMPCOMM, "success")
                    refreshPatchInfusionInfo()
                }, { e ->
                    aapsLogger.debug(LTag.PUMPCOMM, "error : $e")
                })
    }

    fun observeProfile() {
        compositeDisposable += carelevoPatch.profile
            .observeOn(aapsSchedulers.main)
            .subscribe {
                val basalRate = it?.getOrNull()?.getBasal() ?: 0.0
                _basalRate.value = basalRate
                _basalRateFlow.value = basalRate
            }
    }

    fun initUnacknowledgedAlarms() {
        _hasUnacknowledgedAlarms.value = false
    }

    fun triggerEvent(event: Event) {
        viewModelScope.launch {
            when (event) {
                is CarelevoOverviewEvent -> generateEventType(event).run { _event.emit(this) }
            }
        }
    }

    private fun generateEventType(event: Event): Event {
        return when (event) {
            is CarelevoOverviewEvent.ShowMessageBluetoothNotEnabled    -> event
            is CarelevoOverviewEvent.ShowMessageCarelevoIsNotConnected -> event
            is CarelevoOverviewEvent.DiscardComplete                   -> event
            is CarelevoOverviewEvent.DiscardFailed                     -> event
            is CarelevoOverviewEvent.ResumePumpComplete                -> event
            is CarelevoOverviewEvent.ResumePumpFailed                  -> event
            is CarelevoOverviewEvent.StopPumpComplete                  -> event
            is CarelevoOverviewEvent.StopPumpFailed                    -> event
            is CarelevoOverviewEvent.StartConnectionFlow               -> event
            is CarelevoOverviewEvent.StartCommunicationCheck           -> event
            is CarelevoOverviewEvent.ShowPumpDiscardDialog             -> event

            is CarelevoOverviewEvent.ClickPumpStopResumeBtn            -> {
                resolvePumpStopResumeEvent()
            }

            else                                                       -> CarelevoOverviewEvent.NoAction
        }
    }

    private fun resolvePumpStopResumeEvent(): CarelevoOverviewEvent {
        return when (carelevoPatch.resolvePatchState()) {
            is PatchState.NotConnectedNotBooting -> {
                CarelevoOverviewEvent.ShowMessageCarelevoIsNotConnected
            }

            else                                 -> {
                val isStop = carelevoPatch.patchInfo.value?.get()?.isStopped ?: false
                if (isStop) {
                    CarelevoOverviewEvent.ShowPumpResumeDialog
                } else {
                    CarelevoOverviewEvent.ShowPumpStopDurationSelectDialog
                }
            }
        }
    }

    private fun setUiState(state: State) {
        viewModelScope.launch {
            _uiState.tryEmit(state)
        }
    }

    fun startDiscardProcess() {
        when (carelevoPatch.patchState.value?.getOrNull()) {
            is PatchState.NotConnectedNotBooting, null -> {
                triggerEvent(CarelevoOverviewEvent.DiscardComplete)
            }

            else                                       -> {
                // Route the BLE stop through the queue (reconnect-before-execute); if the patch can't
                // be reached at all, fall back to the DB-only force-discard.
                setUiState(UiState.Loading)
                viewModelScope.launch {
                    val result = commandQueue.customCommand(CmdDiscard())
                    if (result.success) {
                        aapsLogger.debug(LTag.PUMPCOMM, "[startDiscard] success")
                        bleController.unBondDevice()
                        carelevoPatch.releasePatch()
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoOverviewEvent.DiscardComplete)
                    } else {
                        aapsLogger.error(LTag.PUMPCOMM, "[startDiscard] failed, falling back to force-discard")
                        startPatchForceDiscard()
                    }
                }
            }
        }
    }

    private fun handlePatchDiscardResponse(response: ResponseResult<*>) {
        when (response) {
            is ResponseResult.Success -> {
                aapsLogger.debug(LTag.PUMPCOMM, "[startPatchDiscard] success")
                bleController.unBondDevice()
                carelevoPatch.releasePatch()
                triggerEvent(CarelevoOverviewEvent.DiscardComplete)
            }

            else                      -> {
                aapsLogger.debug(LTag.PUMPCOMM, "[startPatchDiscard] failed or error")
                triggerEvent(CarelevoOverviewEvent.DiscardFailed)
            }
        }
        setUiState(UiState.Idle)
    }

    private fun handlePatchDiscardError(error: Throwable) {
        aapsLogger.debug(LTag.PUMPCOMM, "[startPatchDiscard] error: $error")
        setUiState(UiState.Idle)
        triggerEvent(CarelevoOverviewEvent.DiscardFailed)
    }

    private fun startPatchForceDiscard() {
        setUiState(UiState.Loading)
        compositeDisposable += patchForceDiscardUseCase.execute()
            .timeout(10, TimeUnit.SECONDS)
            .subscribeOn(aapsSchedulers.io)
            .observeOn(aapsSchedulers.main)
            .subscribe(
                { response -> handlePatchDiscardResponse(response) },
                { error -> handlePatchDiscardError(error) }
            )
    }

    fun startPumpStopProcess(stopMinute: Int) {
        if (!carelevoPatch.isBluetoothEnabled()) {
            triggerEvent(CarelevoOverviewEvent.ShowMessageBluetoothNotEnabled)
            return
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            triggerEvent(CarelevoOverviewEvent.ShowMessageCarelevoIsNotConnected)
            return
        }

        setUiState(UiState.Loading)

        val infusionInfo = carelevoPatch.infusionInfo.value?.getOrNull()
        val isExtendBolusRunning = infusionInfo?.extendBolusInfusionInfo != null
        val isTempBasalRunning = infusionInfo?.tempBasalInfusionInfo != null

        viewModelScope.launch {
            val cancelExtendBolusResult = if (isExtendBolusRunning) {
                cancelExtendBolus()
            } else {
                true
            }
            val cancelTempBasalResult = if (isTempBasalRunning) {
                cancelTempBasal()
            } else {
                true
            }

            aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] isTempBasalRunning=$cancelTempBasalResult, isExtendBolusRunning=$cancelExtendBolusResult, stopMinute: $stopMinute")

            if (cancelExtendBolusResult && cancelTempBasalResult) {
                compositeDisposable += pumpStopUseCase.execute(CarelevoPumpStopRequestModel(durationMin = stopMinute))
                    .timeout(3000L, TimeUnit.MILLISECONDS)
                    .subscribeOn(aapsSchedulers.io)
                    .observeOn(aapsSchedulers.main)
                    .doOnError { e ->
                        aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] doOnError: $e")
                    }
                    .doFinally {
                        setUiState(UiState.Idle)
                    }
                    .subscribe(
                        { response ->
                            when (response) {
                                is ResponseResult.Success -> {
                                    handlePumpStopResponse(
                                        isTempBasalRunning = isTempBasalRunning,
                                        isExtendBolusRunning = isExtendBolusRunning,
                                        stopMinute = stopMinute
                                    )
                                }

                                is ResponseResult.Error   -> {
                                    aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] response error: ${response.e}")
                                    triggerEvent(CarelevoOverviewEvent.StopPumpFailed)
                                }

                                else                      -> {
                                    aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] response failed/unknown")
                                    triggerEvent(CarelevoOverviewEvent.StopPumpFailed)
                                }
                            }
                        },
                        { e ->
                            aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] subscribe throwable: $e")
                            triggerEvent(CarelevoOverviewEvent.StopPumpFailed)
                        })
            } else {
                aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] no active temp/extend bolus to cancel")
                setUiState(UiState.Idle)
                triggerEvent(CarelevoOverviewEvent.StopPumpFailed)
            }
        }
    }

    private fun handlePumpStopResponse(
        isTempBasalRunning: Boolean,
        isExtendBolusRunning: Boolean,
        stopMinute: Int
    ) {
        aapsLogger.debug(LTag.PUMPCOMM, "[startPumpStopProcess] response success")

        viewModelScope.launch {
            pumpSync.syncTemporaryBasalWithPumpId(
                timestamp = dateUtil.now(),
                rate = PumpRate(0.0),
                duration = T.mins(stopMinute.toLong()).msecs(),
                isAbsolute = true,
                type = PumpSync.TemporaryBasalType.PUMP_SUSPEND,
                pumpId = dateUtil.now(),
                pumpType = PumpType.CAREMEDI_CARELEVO,
                pumpSerial = carelevoPatch.patchInfo.value?.getOrNull()?.manufactureNumber ?: ""
            )

            pumpSync.syncStopExtendedBolusWithPumpId(
                timestamp = dateUtil.now(),
                endPumpId = dateUtil.now(),
                pumpType = PumpType.CAREMEDI_CARELEVO,
                pumpSerial = carelevoPatch.patchInfo.value?.getOrNull()?.manufactureNumber ?: ""
            )
        }

        clearInfusionInfo(
            CarelevoDeleteInfusionRequestModel(
                isDeleteTempBasal = isTempBasalRunning,
                isDeleteImmeBolus = false,
                isDeleteExtendBolus = isExtendBolusRunning
            )
        )

        triggerEvent(CarelevoOverviewEvent.StopPumpComplete)
    }

    private suspend fun cancelTempBasal(): Boolean {
        return commandQueue.cancelTempBasal(true).success
    }

    private suspend fun cancelExtendBolus(): Boolean {
        return commandQueue.cancelExtended().success
    }

    fun startPumpResume() {
        if (!carelevoPatch.isBluetoothEnabled()) {
            triggerEvent(CarelevoOverviewEvent.ShowMessageBluetoothNotEnabled)
            return
        }

        if (!carelevoPatch.isCarelevoConnected()) {
            triggerEvent(CarelevoOverviewEvent.ShowMessageCarelevoIsNotConnected)
            return
        }

        setUiState(UiState.Loading)
        compositeDisposable += pumpResumeUseCase.execute()
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .observeOn(aapsSchedulers.io)
            .subscribeOn(aapsSchedulers.io)
            .doOnError {
                aapsLogger.debug(LTag.PUMPCOMM, "doOnError called : $it")
                setUiState(UiState.Idle)
                triggerEvent(CarelevoOverviewEvent.ResumePumpFailed)
            }
            .subscribe { response ->
                when (response) {
                    is ResponseResult.Success -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response success")
                        viewModelScope.launch {
                            pumpSync.syncStopTemporaryBasalWithPumpId(
                                timestamp = dateUtil.now(),
                                endPumpId = dateUtil.now(),
                                pumpType = PumpType.CAREMEDI_CARELEVO,
                                pumpSerial = carelevoPatch.patchInfo.value?.getOrNull()?.manufactureNumber ?: ""
                            )
                        }

                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoOverviewEvent.ResumePumpComplete)
                    }

                    is ResponseResult.Failure -> {}

                    is ResponseResult.Error   -> {
                        aapsLogger.debug(LTag.PUMPCOMM, "response failed: ${response.e.message}")
                        setUiState(UiState.Idle)
                        triggerEvent(CarelevoOverviewEvent.ResumePumpFailed)
                    }
                }
            }
    }

    fun parseBootDateTime(raw: String?): LocalDateTime? {
        if (raw.isNullOrBlank()) {
            return null
        }
        return try {
            val formatter = DateTimeFormatter.ofPattern("yyMMddHHmm")
            LocalDateTime.parse(raw, formatter)
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    fun parseBootDateTime(utcMillis: Long?): LocalDateTime? {
        if (utcMillis == null) {
            return null
        }

        return runCatching {
            LocalDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneId.systemDefault())
        }.getOrNull()
    }

    private fun onDisconnectValue() {
        _overviewDataFlow.value = defaultOverviewData()
        _serialNumber.value = ""
        _lotNumber.value = ""
        _bootDateTime.value = ""
        _expirationTime.value = ""
        _insulinRemains.value = ""
        _totalInsulinAmount.value = 0.0
        _isPumpStop.value = false
        _runningRemainMinutes.value = 0
        _tempBasalRate.value = null
        _tempBasalRateFlow.value = null
        _basalRate.value = 0.0
        _basalRateFlow.value = 0.0
    }

    private fun defaultOverviewData(): CarelevoOverviewUiModel = CarelevoOverviewUiModel(
        serialNumber = "",
        lotNumber = "",
        bootDateTimeUi = "",
        expirationTime = "",
        infusionStatus = null,
        insulinRemainText = "",
        totalBasal = 0.0,
        totalBolus = 0.0,
        isPumpStopped = false,
        runningRemainMinutes = 0
    )

    private fun buildOverviewState(
        bluetoothState: DeviceModuleState?,
        patchState: PatchState?,
        overviewData: CarelevoOverviewUiModel,
        basalRate: Double,
        tempBasalRate: Double?
    ): PumpOverviewUiState {
        // When connected+idle show no pump banner so the shared communication/queue status
        // can surface (matches Medtrum/Equil); otherwise show the disconnected warning.
        val banner = when (patchState) {
            PatchState.ConnectedBooted        -> null

            PatchState.NotConnectedNotBooting -> StatusBanner(
                text = rh.gs(R.string.carelevo_state_none_value),
                level = StatusLevel.WARNING
            )

            else                              -> StatusBanner(
                text = rh.gs(R.string.carelevo_state_disconnected_value),
                level = StatusLevel.WARNING
            )
        } ?: communicationStatus.statusBanner()

        val infoRows = buildList {
            add(
                PumpInfoRow(
                    label = rh.gs(R.string.carelevo_bluetooth_state_key),
                    value = bluetoothLabel(bluetoothState)
                )
            )

            when (patchState) {
                PatchState.NotConnectedNotBooting -> Unit

                PatchState.NotConnectedBooted     -> {
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_serial_number_key),
                            value = overviewData.serialNumber.ifBlank { "-" }
                        )
                    )
                }

                PatchState.ConnectedBooted        -> {
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_serial_number_key),
                            value = overviewData.serialNumber.ifBlank { "-" }
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_firmware_version_key),
                            value = overviewData.lotNumber.ifBlank { "-" }
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_boot_date_time_key),
                            value = overviewData.bootDateTimeUi.ifBlank { "-" }
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_expiration_key),
                            value = overviewData.expirationTime.ifBlank { "-" }
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_running_remain_time),
                            value = formatRemainingMinutes(overviewData.runningRemainMinutes)
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_basal_rate_key),
                            value = rh.gs(R.string.common_label_unit_value_dose_per_speed_with_space, basalRate)
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_temp_basal_rate_key),
                            value = rh.gs(R.string.common_label_unit_value_dose_per_speed_with_space, tempBasalRate ?: 0.0)
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_insulin_remain_key),
                            value = overviewData.insulinRemainText.ifBlank { "-" }
                        )
                    )
                    add(
                        PumpInfoRow(
                            label = rh.gs(R.string.carelevo_total_insulin_key),
                            value = rh.gs(
                                R.string.common_label_unit_value_dose_with_space,
                                String.format(Locale.US, "%.2f", overviewData.totalBasal + overviewData.totalBolus)
                            )
                        )
                    )
                }

                null                              -> Unit
                else                              -> Unit
            }
        }

        val primaryActions = when (patchState) {
            PatchState.NotConnectedNotBooting -> listOf(
                PumpAction(
                    label = rh.gs(R.string.carelevo_overview_connect_btn_label),
                    icon = Icons.Filled.SwapHoriz,
                    category = ActionCategory.PRIMARY,
                    onClick = { triggerEvent(CarelevoOverviewEvent.StartConnectionFlow) }
                )
            )

            PatchState.NotConnectedBooted     -> listOf(
                PumpAction(
                    label = rh.gs(R.string.carelevo_overview_communication_btn_label),
                    icon = Icons.Filled.SwapHoriz,
                    category = ActionCategory.PRIMARY,
                    onClick = { triggerEvent(CarelevoOverviewEvent.StartCommunicationCheck) }
                )
            )

            else                              -> emptyList()
        }

        val managementActions = if (patchState == PatchState.ConnectedBooted) {
            listOf(
                PumpAction(
                    label = rh.gs(R.string.carelevo_overview_pump_discard_btn_label),
                    icon = Icons.Filled.Delete,
                    category = ActionCategory.MANAGEMENT,
                    onClick = { triggerEvent(CarelevoOverviewEvent.ShowPumpDiscardDialog) }
                ),
                PumpAction(
                    label = if (overviewData.isPumpStopped) {
                        rh.gs(CoreUiR.string.pump_resume)
                    } else {
                        rh.gs(CoreUiR.string.pump_suspend)
                    },
                    icon = if (overviewData.isPumpStopped) Icons.Filled.PlayArrow else IcLoopPaused,
                    category = ActionCategory.MANAGEMENT,
                    onClick = { triggerEvent(CarelevoOverviewEvent.ClickPumpStopResumeBtn) }
                )
            )
        } else {
            emptyList()
        }

        return PumpOverviewUiState(
            statusBanner = banner,
            queueStatus = communicationStatus.queueStatus(),
            infoRows = infoRows,
            primaryActions = primaryActions,
            managementActions = managementActions
        )
    }

    private fun patchStateLabel(patchState: PatchState?): String = when (patchState) {
        PatchState.NotConnectedNotBooting -> rh.gs(R.string.carelevo_state_none_value)
        PatchState.ConnectedBooted        -> rh.gs(R.string.carelevo_state_connected_value)
        else                              -> rh.gs(R.string.carelevo_state_disconnected_value)
    }

    private fun bluetoothLabel(bluetoothState: DeviceModuleState?): String = when (bluetoothState) {
        DeviceModuleState.DEVICE_STATE_ON  -> rh.gs(R.string.carelevo_state_connected_value)
        DeviceModuleState.DEVICE_STATE_OFF -> rh.gs(R.string.carelevo_state_disconnected_value)
        else                               -> "-"
    }

    private fun formatRemainingMinutes(totalMinutes: Int): String {
        if (totalMinutes <= 0) return "-"

        val days = totalMinutes / 1440
        val remainingMinutesAfterDays = totalMinutes % 1440
        val hours = remainingMinutesAfterDays / 60
        val minutes = remainingMinutesAfterDays % 60

        return if (days > 0) {
            rh.gs(R.string.common_unit_value_day_hour_min, days, hours, minutes)
        } else {
            String.format(Locale.getDefault(), "%02d:%02d", hours, minutes)
        }
    }

    private data class OverviewInputs(
        val bluetoothState: DeviceModuleState?,
        val patchState: PatchState,
        val overviewData: CarelevoOverviewUiModel,
        val basalRate: Double,
        val tempBasalRate: Double?
    )

    private fun getRemainMin(createdAt: LocalDateTime): Int {
        val endAt = createdAt.plusDays(7)
        var remainMin = ChronoUnit.MINUTES.between(LocalDateTime.now(), endAt)

        if (LocalDateTime.now().isAfter(endAt)) {
            remainMin = ChronoUnit.MINUTES.between(endAt, LocalDateTime.now())
        }

        return remainMin.toInt()
    }

    private fun getExpireAtText(createdAt: LocalDateTime): String {
        val now = LocalDateTime.now()
        val baseEnd = createdAt.plusDays(7)

        val expireAt = if (now.isAfter(baseEnd)) {
            baseEnd.plusHours(12)
        } else {
            baseEnd
        }

        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
        return expireAt.format(formatter)
    }

    fun refreshPatchInfusionInfo() {
        if (!carelevoPatch.isBluetoothEnabled()) {
            return
        }
        if (!carelevoPatch.isCarelevoConnected()) {
            return
        }

        compositeDisposable += requestPatchInfusionInfoUseCase.execute()
            .observeOn(aapsSchedulers.main)
            .subscribeOn(aapsSchedulers.io)
            .timeout(3000L, TimeUnit.MILLISECONDS)
            .subscribe()
    }

    override fun onCleared() {
        compositeDisposable.clear()
        super.onCleared()
    }
}
