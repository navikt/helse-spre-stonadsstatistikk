package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")

internal class UtbetaltRiver(
    rapidsConnection: RapidsConnection,
    private val utbetaltDao: UtbetaltDao,
    private val dokumentDao: DokumentDao,
    private val stønadProducer: KafkaProducer<String, String>
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

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        val vedtak = Vedtak(
            hendelseId = UUID.fromString(packet["@id"].asText()),
            fødselsnummer = fødselsnummer,
            orgnummer = packet["organisasjonsnummer"].asText(),
            dokumenter = packet["hendelser"].toDokumenter(),
            oppdrag = packet["utbetalt"].toOppdrag(),
            fom = packet["fom"].asLocalDate(),
            tom = packet["tom"].asLocalDate(),
            forbrukteSykedager = packet["forbrukteSykedager"].asInt(),
            gjenståendeSykedager = packet["gjenståendeSykedager"].asInt(),
            maksdato = packet["maksdato"].asOptionalLocalDate(),
            opprettet = packet["@opprettet"].asLocalDateTime()
        )
        utbetaltDao.opprett(vedtak)
        //Map til stønadsformat
        val stønad: UtbetaltEvent = vedtak.toUtbetalt()
        //Send til kafka
        stønadProducer.send(ProducerRecord("aapen-sykepenger-stønad", null, fødselsnummer, objectMapper.writeValueAsString(stønad)))
        log.info("Utbetaling på ${packet["aktørId"]} lagret")
    }

    private fun Vedtak.toUtbetalt() = UtbetaltEvent(
        fødselsnummer = fødselsnummer,
        organisasjonsnummer = orgnummer,
        sykmeldingId = dokumenter.sykmelding.dokumentId,
        soknadId = dokumenter.søknad.dokumentId,
        inntektsmeldingId = dokumenter.inntektsmelding?.dokumentId,
        oppdrag = oppdrag.filter{ oppdrag -> oppdrag.utbetalingslinjer.isNotEmpty() }.map { oppdrag ->
            UtbetaltEvent.Utbetalt(
                mottaker = oppdrag.mottaker,
                fagområde = oppdrag.fagområde,
                fagsystemId = oppdrag.fagsystemId,
                totalbeløp = oppdrag.totalbeløp,
                utbetalingslinjer = oppdrag.utbetalingslinjer.map { linje ->
                    UtbetaltEvent.Utbetalt.Utbetalingslinje(
                        fom = linje.fom,
                        tom = linje.tom,
                        dagsats = linje.dagsats,
                        beløp = linje.beløp,
                        grad = linje.grad,
                        sykedager = linje.sykedager
                    )
                }
            )
        },
        fom = fom,
        tom = tom,
        forbrukteSykedager = forbrukteSykedager,
        gjenståendeSykedager = gjenståendeSykedager,
        maksdato = maksdato,
        sendtTilUtbetalingTidspunkt = opprettet
    )

    private fun JsonNode.toDokumenter() =
        dokumentDao.finnDokumenter(map { UUID.fromString(it.asText()) })

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
