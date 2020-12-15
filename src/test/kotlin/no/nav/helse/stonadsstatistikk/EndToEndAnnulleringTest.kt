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

internal class EndToEndAnnulleringTest {
    private val testRapid = TestRapid()
    private val dataSource = testDataSource()
    private val dokumentDao = mockk<DokumentDao>()
    private val utbetaltDao = mockk<UtbetaltDao>()
    private val utbetaltBehovDao = mockk<UtbetaltBehovDao>()
    private val annulleringDao = AnnulleringDao(dataSource)
    private val kafkaStønadProducer: KafkaProducer<String, String> = mockk(relaxed = true)
    private val utbetaltService = UtbetaltService(utbetaltDao, dokumentDao, utbetaltBehovDao, annulleringDao, kafkaStønadProducer)

    init {
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
}
