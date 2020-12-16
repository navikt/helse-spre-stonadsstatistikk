package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.util.KtorExperimentalAPI
import no.nav.helse.rapids_rivers.RapidApplication
import org.apache.kafka.clients.producer.KafkaProducer

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
    val utbetaltBehovDao = UtbetaltBehovDao(dataSource)
    val annulleringDao = AnnulleringDao(dataSource)
    val producer =
        KafkaProducer<String, String>(
            loadBaseConfig(
                env.raw.getValue("KAFKA_BOOTSTRAP_SERVERS"),
                env.serviceUser
            ).toProducerConfig()
        )
    val utbetaltService = UtbetaltService(utbetaltDao, dokumentDao, utbetaltBehovDao, annulleringDao, producer)

    RapidApplication.Builder(RapidApplication.RapidApplicationConfig.fromEnv(env.raw))
        .build()
        .apply {
            NyttDokumentRiver(this, dokumentDao)
            TilUtbetalingBehovRiver(this, utbetaltBehovDao)
            UtbetaltRiver(this, utbetaltService)
            UtbetaltUtenMaksdatoRiver(this, utbetaltService)
            OldUtbetalingRiver(this, utbetaltService)
            AnnullertRiver(this, utbetaltService)
            start()
        }
}
