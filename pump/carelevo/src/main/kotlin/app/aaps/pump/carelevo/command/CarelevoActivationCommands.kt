package app.aaps.pump.carelevo.command

import app.aaps.core.interfaces.queue.CustomCommand

/**
 * Activation operations routed through the AAPS CommandQueue (managed connect-before-execute /
 * reconnect). See [CarelevoActivationExecutor]. Each is a marker command; the executor runs the
 * corresponding use case blocking on the queue thread and returns the result.
 */

class CmdNeedleCheck : CustomCommand {

    override val statusDescription: String = "NEEDLE CHECK"
}

class CmdSetBasal : CustomCommand {

    override val statusDescription: String = "SET BASAL"
}

class CmdAdditionalPriming : CustomCommand {

    override val statusDescription: String = "ADDITIONAL PRIMING"
}

class CmdDiscard : CustomCommand {

    override val statusDescription: String = "DISCARD PATCH"
}
