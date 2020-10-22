package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
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
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("linjer") { array ->
                    require(array.isArray && !array.isEmpty)
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val fagsystemId = packet["fagsystemId"].asText()
        val opprettet = packet["@opprettet"].asLocalDateTime()

        utbetaltService.håndter(Annullering(fødselsnummer, fagsystemId, opprettet))

        log.info("Vedtak med fagsystemId $fagsystemId er annullert")
    }
}
