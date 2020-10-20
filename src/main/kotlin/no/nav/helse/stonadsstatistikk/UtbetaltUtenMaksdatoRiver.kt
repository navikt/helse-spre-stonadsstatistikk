package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")

internal class UtbetaltUtenMaksdatoRiver(
        rapidsConnection: RapidsConnection,
        private val utbetaltService: UtbetaltService
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey(
                    "aktørId",
                    "fødselsnummer",
                    "@id",
                    "organisasjonsnummer",
                    "hendelser",
                    "utbetalt",
                    "forbrukteSykedager",
                    "gjenståendeSykedager"
                )
                it.require("fom", JsonNode::asLocalDate)
                it.require("tom", JsonNode::asLocalDate)
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.interestedIn("maksdato")
            }
        }.register(this)
    }

    class Vedtak(
            val hendelseId: UUID,
            val fødselsnummer: String,
            val orgnummer: String,
            val hendelser: List<UUID>,
            val oppdrag: List<Oppdrag>,
            val fom: LocalDate,
            val tom: LocalDate,
            val forbrukteSykedager: Int,
            val gjenståendeSykedager: Int,
            val opprettet: LocalDateTime
    ) {
        class Oppdrag(
            val mottaker: String,
            val fagområde: String,
            val fagsystemId: String,
            val totalbeløp: Int,
            val utbetalingslinjer: List<Utbetalingslinje>
        ) {
            class Utbetalingslinje(
                    val fom: LocalDate,
                    val tom: LocalDate,
                    val dagsats: Int,
                    val beløp: Int,
                    val grad: Double,
                    val sykedager: Int
            )
        }
    }


    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        if(!packet["maksdato"].isMissingNode) return
        val vedtak = Vedtak(
            hendelseId = UUID.fromString(packet["@id"].asText()),
            fødselsnummer = packet["fødselsnummer"].asText(),
            orgnummer = packet["organisasjonsnummer"].asText(),
            hendelser = packet["hendelser"].map { UUID.fromString(it.asText()) },
            oppdrag = packet["utbetalt"].toOppdrag(),
            fom = packet["fom"].asLocalDate(),
            tom = packet["tom"].asLocalDate(),
            forbrukteSykedager = packet["forbrukteSykedager"].asInt(),
            gjenståendeSykedager = packet["gjenståendeSykedager"].asInt(),
            opprettet = packet["@opprettet"].asLocalDateTime()
        )

        utbetaltService.håndter(vedtak)
        log.info("Utbetaling uten maksdato på ${packet["aktørId"]} håndtert")
    }

    private fun JsonNode.toOppdrag() = map {
        Vedtak.Oppdrag(
            mottaker = it["mottaker"].asText(),
            fagområde = it["fagområde"].asText(),
            fagsystemId = it["fagsystemId"].asText(),
            totalbeløp = it["totalbeløp"].asInt(),
            utbetalingslinjer = it["utbetalingslinjer"].toUtbetalingslinjer()
        )
    }

    private fun JsonNode.toUtbetalingslinjer() = map {
        Vedtak.Oppdrag.Utbetalingslinje(
            fom = it["fom"].asLocalDate(),
            tom = it["tom"].asLocalDate(),
            dagsats = it["dagsats"].asInt(),
            beløp = it["beløp"].asInt(),
            grad = it["grad"].asDouble(),
            sykedager = it["sykedager"].asInt()
        )
    }
}
