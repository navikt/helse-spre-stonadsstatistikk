package no.nav.helse.stonadsstatistikk

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

data class Dokumenter(
    val sykmelding: Hendelse,
    val søknad: Hendelse,
    val inntektsmelding: Hendelse?
) {
    init {
        require(sykmelding.type == Dokument.Sykmelding)
        require(søknad.type == Dokument.Søknad)
        inntektsmelding?.also { require(it.type == Dokument.Inntektsmelding) }
    }
}

class UtbetaltDao(val datasource: DataSource) {
    fun opprett(hendelseId: UUID, vedtak: UtbetaltEvent) {
        sessionOf(datasource, true).use { session ->
            session.transaction {
                it.opprett(hendelseId, vedtak)
            }
        }
    }

    private fun Session.opprett(hendelseId: UUID, vedtak: UtbetaltEvent) {
        @Language("PostgreSQL")
        val query = """INSERT INTO vedtak(
            fodselsnummer,
            organisasjonsnummer,
            utbetalingstidspunkt,
            fom,
            tom,
            forbrukte_sykedager,
            gjenstaende_sykedager,
            maksdato,
            sykmelding_id,
            soknad_id,
            inntektsmelding_id,
            hendelse_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"""
        val key = run(
            queryOf(
                query,
                vedtak.fødselsnummer,
                vedtak.organisasjonsnummer,
                vedtak.utbetalingstidspunkt,
                vedtak.fom,
                vedtak.tom,
                vedtak.forbrukteSykedager,
                vedtak.gjenståendeSykedager,
                vedtak.maksdato,
                vedtak.sykmeldingId,
                vedtak.soknadId,
                vedtak.inntektsmeldingId,
                hendelseId
            ).asUpdateAndReturnGeneratedKey
        )
        opprettOppdrag(requireNotNull(key), vedtak.oppdrag)
    }

    private fun Session.opprettOppdrag(vedtakKey: Long, oppdragListe: List<UtbetaltEvent.Utbetalt>) {
        @Language("PostgreSQL")
        val query = """INSERT INTO oppdrag(vedtak_id, mottaker, fagomrade, fagsystem_id, totalbelop)
            VALUES(?,?,?,?,?)"""
        oppdragListe.forEach { oppdrag ->
            val key = run(
                queryOf(
                    query,
                    vedtakKey,
                    oppdrag.mottaker,
                    oppdrag.fagområde,
                    oppdrag.fagsystemId,
                    oppdrag.totalbeløp
                ).asUpdateAndReturnGeneratedKey
            )
            opprettUtbetalingslinjer(requireNotNull(key), oppdrag.utbetalingslinjer)
        }
    }

    private fun Session.opprettUtbetalingslinjer(oppdragKey: Long, linjeListe: List<UtbetaltEvent.Utbetalt.Utbetalingslinje>) {
        @Language("PostgreSQL")
        val query = """INSERT INTO utbetaling(oppdrag_id, fom, tom, dagsats, grad, belop, sykedager)
            VALUES(?,?,?,?,?,?,?)"""
        linjeListe.forEach { linje ->
            run(
                queryOf(
                    query,
                    oppdragKey,
                    linje.fom,
                    linje.tom,
                    linje.dagsats,
                    linje.grad,
                    linje.beløp,
                    linje.sykedager
                ).asUpdate
            )
        }
    }

    fun hentUtbetalinger(): List<UtbetaltEvent> {
        @Language("PostgreSQL")
        val vedtakQuery = "SELECT * FROM vedtak"

        @Language("PostgreSQL")
        val oppdragQuery = "SELECT * FROM oppdrag"

        @Language("PostgreSQL")
        val linjeQuery = "SELECT * FROM utbetaling"

        return sessionOf(datasource, true).use { session ->
            val linjer = session.run(queryOf(linjeQuery).map {
                it.int("oppdrag_id") to UtbetaltEvent.Utbetalt.Utbetalingslinje(
                    fom = it.localDate("fom"),
                    tom = it.localDate("tom"),
                    dagsats = it.int("dagsats"),
                    beløp = it.int("belop"),
                    grad = it.double("grad"),
                    sykedager = it.int("sykedager")
                )
            }.asList).groupBy({ it.first }) { it.second }
            val oppdrag = session.run(queryOf(oppdragQuery).map {
                it.int("vedtak_id") to UtbetaltEvent.Utbetalt(
                    mottaker = it.string("mottaker"),
                    fagområde = it.string("fagomrade"),
                    fagsystemId = it.string("fagsystem_id"),
                    totalbeløp = it.int("totalbelop"),
                    utbetalingslinjer = linjer.getOrDefault(it.int("id"), emptyList())
                )
            }.asList).groupBy({ it.first }) { it.second }
            session.run(queryOf(vedtakQuery).map {
                UtbetaltEvent(
                    fødselsnummer = it.string("fodselsnummer"),
                    organisasjonsnummer = it.string("organisasjonsnummer"),
                    sykmeldingId = UUID.fromString(it.string("sykmelding_id")),
                    soknadId = UUID.fromString(it.string("soknad_id")),
                    inntektsmeldingId = it.stringOrNull("inntektsmelding_id")?.let(UUID::fromString),
                    oppdrag = oppdrag.getOrDefault(it.int("id"), emptyList()),
                    fom = it.localDate("fom"),
                    tom = it.localDate("tom"),
                    forbrukteSykedager = it.int("forbrukte_sykedager"),
                    gjenståendeSykedager = it.int("gjenstaende_sykedager"),
                    maksdato = it.localDateOrNull("maksdato"),
                    utbetalingstidspunkt = it.localDateTime("utbetalingstidspunkt")
                )
            }.asList)
        }
    }
}
