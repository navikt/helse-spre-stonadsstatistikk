package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.util.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.producer.KafkaProducer
import java.io.File
import java.io.FileNotFoundException

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

    seedApp(env.raw, utbetaltService)

    RapidApplication.Builder(RapidApplication.RapidApplicationConfig.fromEnv(env.raw))
        .build()
        .apply {
            NyttDokumentRiver(this, dokumentDao)
            TilUtbetalingBehovRiver(this, utbetaltBehovDao)
            UtbetaltRiver(this, utbetaltService)
            UtbetaltUtenMaksdatoRiver(this, utbetaltService)
            OldUtbetalingRiver(this, utbetaltService)
            UtbetalingBehovAnnullertRiver(this, utbetaltService)
            start()
        }
}

private fun seedApp(env: Map<String, String>, utbetaltService: UtbetaltService) {
    val kafkaConfig = KafkaConfig(
        bootstrapServers = env.getValue("KAFKA_BOOTSTRAP_SERVERS"),
        consumerGroupId = env.getValue("KAFKA_CONSUMER_GROUP_ID") + "-reseed",
        username = "/var/run/secrets/nais.io/service_user/username".readFile(),
        password = "/var/run/secrets/nais.io/service_user/password".readFile(),
        truststore = env["NAV_TRUSTSTORE_PATH"],
        truststorePassword = env["NAV_TRUSTSTORE_PASSWORD"],
        autoOffsetResetConfig = "earliest"
    )

    val seedApp = KafkaRapid.create(kafkaConfig, env.getValue("KAFKA_RAPID_TOPIC"))
        .apply {
            Runtime.getRuntime().addShutdownHook(Thread(this::stop))
            AnnullertRiver(this, utbetaltService)
        }

    GlobalScope.launch { seedApp.start() }
}

private fun String.readFile() =
    try {
        File(this).readText(Charsets.UTF_8)
    } catch (err: FileNotFoundException) {
        null
    }
