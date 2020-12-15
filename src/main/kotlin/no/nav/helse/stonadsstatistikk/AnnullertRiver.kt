package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class AnnullertRiver(
    rapidsConnection: RapidsConnection,
    private val utbetaltService: UtbetaltService
): River.PacketListener {

    private val logg: Logger = LoggerFactory.getLogger("stonadsstatistikk")
    private val sikkerLogg: Logger = LoggerFactory.getLogger("tjenestekall")

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "utbetaling_annullert")
                it.requireKey("fødselsnummer", "aktørId", "fagsystemId")
                it.require("annullertAvSaksbehandler", JsonNode::asLocalDateTime)
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val fagsystemId = packet["fagsystemId"].asText()
        val tidspunkt = packet["annullertAvSaksbehandler"].asLocalDateTime()
        utbetaltService.håndter(Annullering(fødselsnummer, fagsystemId, tidspunkt))
        logg.info("Annullering med fagsystemId=$fagsystemId på ${packet["aktørId"]} håndtert")
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        sikkerLogg.error("Forstod ikke utbetaling_annullert:\n" + problems.toExtendedReport())
    }
}
