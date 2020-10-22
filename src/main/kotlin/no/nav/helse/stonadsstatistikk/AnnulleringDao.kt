package no.nav.helse.stonadsstatistikk

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import javax.sql.DataSource

class AnnulleringDao(val datasource: DataSource) {
    fun opprett(annullering: Annullering) {
        @Language("PostgreSQL")
        val insertAnnullering = "INSERT INTO annullering(fodselsnummer, fagsystem_id, annulleringstidspunkt) VALUES(?,?,?) ON CONFLICT DO NOTHING"
        sessionOf(datasource).use { session ->
            session.run(
                queryOf(insertAnnullering, annullering.fødselsnummer, annullering.fagsystemId, annullering.annulleringstidspunkt).asUpdate
            )
        }
    }

    @Language("PostgreSQL")
    fun hentAnnulleringer(): List<Annullering> {
        return sessionOf(datasource, true).use { session ->
            session.run(queryOf("SELECT * FROM annullering").map {
                Annullering(
                    fødselsnummer = it.string("fodselsnummer"),
                    fagsystemId = it.string("fagsystem_id"),
                    annulleringstidspunkt = it.localDateTime("annulleringstidspunkt")
                )
            }.asList)
        }
    }
}
