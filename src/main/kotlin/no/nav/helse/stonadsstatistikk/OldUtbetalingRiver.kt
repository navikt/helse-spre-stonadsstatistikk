package no.nav.helse.stonadsstatistikk

import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")

internal class OldUtbetalingRiver(
    rapidsConnection: RapidsConnection,
    private val utbetaltService: UtbetaltService
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "utbetalt")
                it.requireKey(
                    "@id",
                    "vedtaksperiodeId",
                    "hendelser",
                    "fødselsnummer",
                    "organisasjonsnummer",
                    "forbrukteSykedager",
                    "@opprettet"
                )
                it.interestedIn("utbetaling", "utbetalingslinjer", "gjenståendeSykedager")
            }
        }.register(this)
    }

    class OldVedtak(
        val hendelseId: UUID,
        val vedtaksperiodeId: UUID,
        val fødselsnummer: String,
        val orgnummer: String,
        val utbetalinger: List<OldUtbetaling>,
        val opprettet: LocalDateTime,
        val forbrukteSykedager: Int,
        val gjenståendeSykedager: Int?,
        val hendelser: List<UUID>
    ) {
        class OldUtbetaling(
            val fom: LocalDate,
            val tom: LocalDate,
            val grad: Double,
            val dagsats: Int,
            val beløp: Int
        )
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val utbetalingslinjer =
            packet["utbetalingslinjer"].takeUnless { it.isMissingOrNull() }?.toList()
                ?: packet["utbetaling"].flatMap { it["utbetalingslinjer"] }
        val vedtaksperiodeId = UUID.fromString(packet["vedtaksperiodeId"].asText())
        val vedtak = OldVedtak(
            hendelseId = UUID.fromString(packet["@id"].asText()),
            vedtaksperiodeId = vedtaksperiodeId,
            fødselsnummer = packet["fødselsnummer"].asText(),
            orgnummer = packet["organisasjonsnummer"].asText(),
            utbetalinger = utbetalingslinjer.map {
                val fom = it["fom"].asLocalDate()
                val tom = it["tom"].asLocalDate()
                val beløp = it["beløp"].asInt()
                OldVedtak.OldUtbetaling(
                    fom = fom,
                    tom = tom,
                    grad = it["grad"].asDouble(),
                    dagsats = it["dagsats"].asInt(),
                    beløp = beløp
                )
            },
            opprettet = packet["@opprettet"].asLocalDateTime(),
            forbrukteSykedager = packet["forbrukteSykedager"].asInt(),
            gjenståendeSykedager = packet["gjenståendeSykedager"].takeUnless { it.isMissingOrNull() }?.asInt(),
            hendelser = packet["hendelser"].map { UUID.fromString(it.asText()) }
        )
        utbetaltService.håndter(vedtak)
        log.info("Håndtert gammelt vedtak med vedtakperiodeId $vedtaksperiodeId")
    }
}
