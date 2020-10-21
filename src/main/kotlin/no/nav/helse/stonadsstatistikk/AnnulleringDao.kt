package no.nav.helse.stonadsstatistikk

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class AnnulleringDao(val datasource: DataSource) {
    fun opprett(annullering: Annullering) {
        @Language("PostgreSQL")
        val insertAnnullering = "INSERT INTO annullering(fodselsnummer, fagsystem_id) VALUES(?,?) ON CONFLICT DO NOTHING"
        sessionOf(datasource).use { session ->
            session.run(
                queryOf(insertAnnullering, annullering.fødselsnummer, annullering.fagsystemId).asUpdate
            )
        }
    }
}
