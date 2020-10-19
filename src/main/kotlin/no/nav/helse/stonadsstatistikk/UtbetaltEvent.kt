package no.nav.helse.stonadsstatistikk

import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

data class UtbetaltEvent(
    val fødselsnummer: String,
    val organisasjonsnummer: String,
    val sykmeldingId: UUID,
    val soknadId: UUID,
    val inntektsmeldingId: UUID?,
    val oppdrag: List<Utbetalt>,
    val fom: LocalDate,
    val tom: LocalDate,
    val forbrukteSykedager: Int,
    val gjenståendeSykedager: Int,
    val maksdato: LocalDate?,
    val sendtTilUtbetalingTidspunkt: LocalDateTime
) {
    data class Utbetalt(
        val mottaker: String,
        val fagområde: String,
        val fagsystemId: String,
        val totalbeløp: Int,
        val utbetalingslinjer: List<Utbetalingslinje>
    ) {
        data class Utbetalingslinje(
            val fom: LocalDate,
            val tom: LocalDate,
            val dagsats: Int,
            val beløp: Int,
            val grad: Double,
            val sykedager: Int
        )
    }
}
