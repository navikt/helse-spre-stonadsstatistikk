package no.nav.helse.stonadsstatistikk

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import java.time.DayOfWeek
import java.time.LocalDate
import kotlin.streams.asSequence

internal class UtbetaltService(
    private val utbetaltDao: UtbetaltDao,
    private val dokumentDao: DokumentDao,
    private val utbetaltBehovDao: UtbetaltBehovDao,
    private val annulleringDao: AnnulleringDao,
    private val stønadProducer: KafkaProducer<String, String>
) {
    internal fun håndter(vedtak: UtbetaltRiver.Vedtak) {
        val dokumenter = dokumentDao.finnDokumenter(vedtak.hendelser)
        val stønad: UtbetaltEvent = vedtak.toUtbetalt(dokumenter)
        utbetaltDao.opprett(vedtak.hendelseId, stønad)
        stønadProducer.send(ProducerRecord(
            "aapen-sykepenger-stønad",
            null,
            vedtak.fødselsnummer,
            objectMapper.writeValueAsString(stønad),
            listOf(RecordHeader("type", "UTBETALING".toByteArray()))
        ))
    }

    private fun UtbetaltRiver.Vedtak.toUtbetalt(dokumenter: Dokumenter) = UtbetaltEvent(
        fødselsnummer = fødselsnummer,
        organisasjonsnummer = orgnummer,
        sykmeldingId = dokumenter.sykmelding.dokumentId,
        soknadId = dokumenter.søknad.dokumentId,
        inntektsmeldingId = dokumenter.inntektsmelding?.dokumentId,
        oppdrag = oppdrag.filter { oppdrag -> oppdrag.utbetalingslinjer.isNotEmpty() }.map { oppdrag ->
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
        utbetalingstidspunkt = opprettet
    )

    internal fun håndter(vedtak: UtbetaltUtenMaksdatoRiver.Vedtak) {
        val dokumenter = dokumentDao.finnDokumenter(vedtak.hendelser)
        val maksdato = utbetaltBehovDao.finnMaksdatoForFagsystemId(vedtak.oppdrag.first { it.utbetalingslinjer.isNotEmpty() }.fagsystemId)
        val stønad: UtbetaltEvent = vedtak.toUtbetalt(dokumenter, maksdato)
        utbetaltDao.opprett(vedtak.hendelseId, stønad)
        stønadProducer.send(ProducerRecord(
            "aapen-sykepenger-stønad",
            null,
            vedtak.fødselsnummer,
            objectMapper.writeValueAsString(stønad),
            listOf(RecordHeader("type", "UTBETALING".toByteArray()))
        ))
    }

    private fun UtbetaltUtenMaksdatoRiver.Vedtak.toUtbetalt(dokumenter: Dokumenter, maksdato: LocalDate) = UtbetaltEvent(
        fødselsnummer = fødselsnummer,
        organisasjonsnummer = orgnummer,
        sykmeldingId = dokumenter.sykmelding.dokumentId,
        soknadId = dokumenter.søknad.dokumentId,
        inntektsmeldingId = dokumenter.inntektsmelding?.dokumentId,
        oppdrag = oppdrag.filter { oppdrag -> oppdrag.utbetalingslinjer.isNotEmpty() }.map { oppdrag ->
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
        utbetalingstidspunkt = opprettet
    )

    internal fun håndter(vedtak: OldUtbetalingRiver.OldVedtak) {
        val dokumenter = dokumentDao.finnDokumenter(vedtak.hendelser)
        val (fagsystemId, maksdato) = utbetaltBehovDao.finnManglendeVerdier(vedtak.vedtaksperiodeId)
        val stønad: UtbetaltEvent = vedtak.toUtbetalt(dokumenter, fagsystemId, maksdato)
        utbetaltDao.opprett(vedtak.hendelseId, stønad)
        stønadProducer.send(ProducerRecord(
            "aapen-sykepenger-stønad",
            null,
            vedtak.fødselsnummer,
            objectMapper.writeValueAsString(stønad),
            listOf(RecordHeader("type", "UTBETALING".toByteArray()))
        ))
    }

    private fun OldUtbetalingRiver.OldVedtak.toUtbetalt(dokumenter: Dokumenter, fagsystemId: String, maksdato: LocalDate) = UtbetaltEvent(
        fødselsnummer = fødselsnummer,
        organisasjonsnummer = orgnummer,
        sykmeldingId = dokumenter.sykmelding.dokumentId,
        soknadId = dokumenter.søknad.dokumentId,
        inntektsmeldingId = dokumenter.inntektsmelding?.dokumentId,
        oppdrag = listOf(
            UtbetaltEvent.Utbetalt(
                mottaker = orgnummer,
                fagområde = "SPREF",
                fagsystemId = fagsystemId,
                totalbeløp = utbetalinger.map { beregnTotalbeløp(it.fom, it.tom, it.beløp) }.sum(),
                utbetalingslinjer = utbetalinger.map { linje ->
                    UtbetaltEvent.Utbetalt.Utbetalingslinje(
                        fom = linje.fom,
                        tom = linje.tom,
                        dagsats = linje.dagsats,
                        beløp = linje.beløp,
                        grad = linje.grad,
                        sykedager = beregnAntallSykedager(linje.fom, linje.tom)
                    )
                }
            )
        ),
        fom = utbetalinger.minOf { it.fom },
        tom = utbetalinger.maxOf { it.tom },
        forbrukteSykedager = forbrukteSykedager,
        gjenståendeSykedager = gjenståendeSykedager
            ?: beregnGjenståendeSykedager(maksdato, utbetalinger.maxOf { it.tom }),
        maksdato = maksdato,
        utbetalingstidspunkt = opprettet
    )

    internal fun håndter(annullering: Annullering) {
        annulleringDao.opprett(annullering)
        stønadProducer.send(ProducerRecord(
            "aapen-sykepenger-stønad",
            null,
            annullering.fødselsnummer,
            objectMapper.writeValueAsString(annullering),
            listOf(RecordHeader("type", "ANNULLERING".toByteArray()))
        ))
    }

    private fun beregnTotalbeløp(fom: LocalDate, tom: LocalDate, beløp: Int) =
        fom.datesUntil(tom.plusDays(1))
            .asSequence()
            .filterNot { it.dayOfWeek in arrayOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) }
            .sumBy { beløp }

    private fun beregnAntallSykedager(fom: LocalDate, tom: LocalDate) =
        fom.datesUntil(tom.plusDays(1))
            .asSequence()
            .count { it.dayOfWeek !in arrayOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) }

    private fun beregnGjenståendeSykedager(maksdato: LocalDate, tom: LocalDate) =
        tom.plusDays(1).datesUntil(maksdato.plusDays(1))
            .asSequence()
            .count { it.dayOfWeek !in arrayOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) }
}
