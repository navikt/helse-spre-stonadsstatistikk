package no.nav.helse.stonadsstatistikk

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")

internal class UtbetalingBehovAnnullertRiver(
    rapidsConnection: RapidsConnection,
    private val utbetaltService: UtbetaltService
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "behov")
                it.requireAll("@behov", listOf("Utbetaling"))
                it.requireValue("@forårsaket_av.event_name", "kanseller_utbetaling")
                it.requireKey("fødselsnummer", "fagsystemId")
                it.require("linjer") { array ->
                    require(array.isArray && !array.isEmpty)
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val fagsystemId = packet["fagsystemId"].asText()

        utbetaltService.håndter(Annullering(fødselsnummer, fagsystemId))

        log.info("Vedtak med fagsystemId $fagsystemId er annullert")
    }
}
