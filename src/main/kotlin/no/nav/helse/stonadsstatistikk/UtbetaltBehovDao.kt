package no.nav.helse.stonadsstatistikk

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.time.LocalDate
import java.util.*
import javax.sql.DataSource

class UtbetaltBehovDao(val datasource: DataSource) {

    fun lagre(vedtaksperiodeId: UUID, fagsystemId: String, maksdato: LocalDate) {
        @Language("PostgreSQL")
        val query = "INSERT INTO vedtak_utbetalingsref(vedtaksperiode_id, utbetalingsref, maksdato) VALUES(?,?,?) ON CONFLICT DO NOTHING"
        sessionOf(datasource).use { session ->
            session.run(
                queryOf(
                    query,
                    vedtaksperiodeId,
                    fagsystemId,
                    maksdato
                ).asUpdate
            )
        }
    }

    fun finnMaksdatoForFagsystemId(fagsystemId: String) = sessionOf(datasource).use { session ->
        @Language("PostgreSQL")
        val query = "SELECT maksdato FROM vedtak_utbetalingsref WHERE utbetalingsref = ?"
        requireNotNull(session.run(
            queryOf(query, fagsystemId).map { row -> row.localDate("maksdato") }.asSingle
        ))
    }

    fun finnMaksdatoForVedtaksperiodeId(vedtaksperiodeId: UUID) = sessionOf(datasource).use { session ->
        @Language("PostgreSQL")
        val query = "SELECT maksdato FROM vedtak_utbetalingsref WHERE vedtaksperiode_id = ?"
        requireNotNull(session.run(
            queryOf(query, vedtaksperiodeId).map { row -> row.localDate("maksdato") }.asSingle
        ))
    }

    fun finnFagsystemIdForVedtaksperiodeId(vedtaksperiodeId: UUID) = sessionOf(datasource).use { session ->
        @Language("PostgreSQL")
        val query = "SELECT utbetalingsref FROM vedtak_utbetalingsref WHERE vedtaksperiode_id = ?"
        requireNotNull(session.run(
            queryOf(query, vedtaksperiodeId).map { row -> row.string("utbetalingsref") }.asSingle
        ))
    }
}
