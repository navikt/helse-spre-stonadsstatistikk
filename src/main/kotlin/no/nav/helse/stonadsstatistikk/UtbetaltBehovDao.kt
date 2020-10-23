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
        val query = "INSERT INTO vedtak_utbetalingsref(vedtaksperiode_id, utbetalingsref, maksdato) VALUES(:vedtaksperiode_id,:utbetalingsref,:maksdato) ON CONFLICT(vedtaksperiode_id) DO UPDATE SET utbetalingsref=:utbetalingsref, maksdato=:maksdato"
        sessionOf(datasource).use { session ->
            session.run(
                queryOf(
                    query,
                    mapOf(
                        "vedtaksperiode_id" to vedtaksperiodeId,
                        "utbetalingsref" to fagsystemId,
                        "maksdato" to maksdato)
                ).asUpdate
            )
        }
    }

    fun finnMaksdatoForFagsystemId(fagsystemId: String) = sessionOf(datasource).use { session ->
        @Language("PostgreSQL")
        val query = "SELECT maksdato FROM vedtak_utbetalingsref WHERE utbetalingsref = ?"
        session.run(
            queryOf(query, fagsystemId).map { row -> row.localDate("maksdato") }.asSingle
        )
    }

    fun finnManglendeVerdier(vedtaksperiodeId: UUID) = sessionOf(datasource).use { session ->
        @Language("PostgreSQL")
        val query = "SELECT utbetalingsref, maksdato FROM vedtak_utbetalingsref WHERE vedtaksperiode_id = ?"
        requireNotNull(session.run(
            queryOf(query, vedtaksperiodeId).map { row -> row.string("utbetalingsref") to row.localDate("maksdato") }.asSingle
        ))
    }
}
