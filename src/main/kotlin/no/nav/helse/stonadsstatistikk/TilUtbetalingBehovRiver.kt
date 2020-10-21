package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")

internal class TilUtbetalingBehovRiver(
    rapidsConnection: RapidsConnection,
    private val utbetaltBehovDao: UtbetaltBehovDao
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "behov")
                it.requireAll("@behov", listOf("Utbetaling"))
                it.requireKey("vedtaksperiodeId", "fagsystemId")
                it.require("maksdato", JsonNode::asLocalDate)
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val fagsystemId = packet["fagsystemId"].asText()
        val maksdato = packet["maksdato"].asLocalDate()

        utbetaltBehovDao.lagre(vedtaksperiodeId, fagsystemId, maksdato)

        log.info("Vedtaksperiode $vedtaksperiodeId er koblet til fagsystemId $fagsystemId")
    }
}
