package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.util.*
import no.nav.helse.rapids_rivers.RapidApplication

val objectMapper = jacksonObjectMapper().apply {
    registerModule(JavaTimeModule())
    disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
}

@KtorExperimentalAPI
fun main() {
    val env = Environment(System.getenv())
    launchApplication(env)
}

@KtorExperimentalAPI
fun launchApplication(env: Environment) {
    val dataSource = DataSourceBuilder(env.db)
        .apply(DataSourceBuilder::migrate)
        .getDataSource()

    val dokumentDao = DokumentDao(dataSource)
    val utbetaltDao = UtbetaltDao(dataSource)

    RapidApplication.Builder(RapidApplication.RapidApplicationConfig.fromEnv(env.raw))
        .build()
        .apply {
//            NyttDokumentRiver(this, dokumentDao)
//            UtbetaltRiver(this, utbetaltDao, dokumentDao)
//            OldUtbetalingRiver(this, vedtakDao, dokumentDao)
//            TilUtbetalingBehovRiver(this, dokumentDao)
//            start()
        }
}
