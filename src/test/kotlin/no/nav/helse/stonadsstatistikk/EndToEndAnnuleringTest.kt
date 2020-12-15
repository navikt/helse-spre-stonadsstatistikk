package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.module.kotlin.readValue
import io.mockk.CapturingSlot
import io.mockk.mockk
import io.mockk.verify
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

private const val FNR = "12020052345"
private const val ORGNUMMER = "987654321"

internal class EndToEndAnnuleringTest {
    private val testRapid = TestRapid()
    private val dataSource = testDataSource()
    private val dokumentDao = mockk<DokumentDao>()
    private val utbetaltDao = mockk<UtbetaltDao>()
    private val utbetaltBehovDao = mockk<UtbetaltBehovDao>()
    private val annulleringDao = AnnulleringDao(dataSource)
    private val kafkaStønadProducer: KafkaProducer<String, String> = mockk(relaxed = true)
    private val utbetaltService = UtbetaltService(utbetaltDao, dokumentDao, utbetaltBehovDao, annulleringDao, kafkaStønadProducer)

    init {
        UtbetalingBehovAnnullertRiver(testRapid, utbetaltService)
        AnnullertRiver(testRapid, utbetaltService)

        Flyway.configure()
            .dataSource(dataSource)
            .load()
            .migrate()
    }

    @BeforeEach
    fun setup() {
        testRapid.reset()
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val query = "TRUNCATE TABLE utbetaling, oppdrag, vedtak, hendelse, vedtak_utbetalingsref, annullering"
            session.run(queryOf(query).asExecute)
        }
    }

    @Test
    fun `håndterer utbetaling_annullert event`() {
        val fødselsnummer = "12345678910"
        val fagsystemId = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        val annullertAvSaksbehandlerTidspunkt = LocalDateTime.of(2020, 1, 1, 1, 1)

        testRapid.sendTestMessage(utbetalingAnnullert(fødselsnummer, fagsystemId, annullertAvSaksbehandlerTidspunkt))

        val capture = CapturingSlot<ProducerRecord<String, String>>()
        verify { kafkaStønadProducer.send(capture(capture)) }

        val record = capture.captured
        val sendtTilStønad = objectMapper.readValue<Annullering>(record.value())
        val event = Annullering(fødselsnummer, fagsystemId, sendtTilStønad.annulleringstidspunkt)
        val lagretAnnullering = annulleringDao.hentAnnulleringer().first()

        assertEquals("ANNULLERING", String(record.headers().headers("type").first().value()))
        assertEquals(event, sendtTilStønad)
        assertEquals(event, lagretAnnullering)
    }

    @Test
    fun `Sender annullering`() {
        val fagsystemId = "VNDG2PFPMNB4FKMC4ORASZ2JJ4"
        testRapid.sendTestMessage(
            utbetalingBehovAnnullert(
                fagsystemId,
                LocalDate.of(2020, 7, 1),
                LocalDate.of(2020, 7, 8)
            )
        )

        val capture = CapturingSlot<ProducerRecord<String, String>>()

        verify { kafkaStønadProducer.send(capture(capture)) }

        val record = capture.captured

        assertEquals("ANNULLERING", String(record.headers().headers("type").first().value()))

        val sendtTilStønad = objectMapper.readValue<Annullering>(record.value())

        val event = Annullering(fødselsnummer = FNR, fagsystemId = fagsystemId, annulleringstidspunkt = sendtTilStønad.annulleringstidspunkt)

        assertEquals(event, sendtTilStønad)

        val lagretAnnullering = annulleringDao.hentAnnulleringer().first()
        assertEquals(event, lagretAnnullering)
    }

    @Language("JSON")
    private fun utbetalingAnnullert(
        fødselsnummer: String,
        fagsystemId: String,
        annullertAvSaksbehandlerTidspunkt: LocalDateTime
    ) = """
        {
          "utbetalingId": "${UUID.randomUUID()}",
          "fagsystemId": "$fagsystemId",
          "utbetalingslinjer": [
            {
              "fom": "2020-01-01",
              "tom": "2020-01-31",
              "beløp": 10000,
              "grad": 100.0
            }
          ],
          "annullertAvSaksbehandler": "$annullertAvSaksbehandlerTidspunkt",
          "saksbehandlerEpost": "saksbehandler@nav.no",
          "@event_name": "utbetaling_annullert",
          "@id": "5132bee3-646d-4992-95a2-5c94cacd0807",
          "@opprettet": "2020-12-15T14:45:00.000000",
          "aktørId": "1111110000000",
          "fødselsnummer": "$fødselsnummer",
          "organisasjonsnummer": "987654321"
        }
    """

    @Language("JSON")
    private fun utbetalingBehovAnnullert(fagsystemId: String, fom: LocalDate, tom: LocalDate) = """{
    "@event_name": "behov",
    "@opprettet": "2020-10-20T14:54:12.156000",
    "@id": "a1d398e3-55b3-4bf6-9aef-2bf3b0c73e18",
    "@behov": [
        "Utbetaling"
    ],
    "@forårsaket_av": {
        "event_name": "kanseller_utbetaling",
        "id": "11448930-1a85-4886-b180-adafec3e8077",
        "opprettet": "2020-10-20T14:54:11.983834"
    },
    "aktørId": "42",
    "fødselsnummer": "$FNR",
    "organisasjonsnummer": "$ORGNUMMER",
    "id": "11448930-1a85-4886-b180-adafec3e8077",
    "vedtaksperiodeId": "7c1c3c20-8cef-4ec3-bc27-5c452229c209",
    "tilstand": "TIL_ANNULLERING",
    "mottaker": "987654321",
    "fagområde": "SPREF",
    "linjer": [
        {
            "fom": "$fom",
            "tom": "$tom",
            "dagsats": 1431,
            "lønn": 1431,
            "grad": 100.0,
            "refFagsystemId": null,
            "delytelseId": 1,
            "datoStatusFom": "2018-01-19",
            "statuskode": "OPPH",
            "refDelytelseId": null,
            "endringskode": "ENDR",
            "klassekode": "SPREFAG-IOP"
        }
    ],
    "fagsystemId": "$fagsystemId",
    "endringskode": "ENDR",
    "sisteArbeidsgiverdag": null,
    "nettoBeløp": 0,
    "saksbehandler": "Ola Nordmann",
    "saksbehandlerEpost": "tbd@nav.no",
    "godkjenttidspunkt": "2020-10-20T14:54:11.983834",
    "annullering": true,
    "system_read_count": 0
}
"""

}
