package no.nav.helse.stonadsstatistikk

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.*
import io.ktor.auth.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.jackson.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.util.*
import no.nav.helse.rapids_rivers.RapidApplication
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.system.measureTimeMillis

private val log: Logger = LoggerFactory.getLogger("stonadsstatistikk")
private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")

val objectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

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
    val vedtakDao = VedtakDao(dataSource)

    RapidApplication.Builder(RapidApplication.RapidApplicationConfig.fromEnv(env.raw))
//        .withKtorModule { stonadsstatistikk(env.auth, dokumentDao, vedtakDao) }
        .build()
        .apply {
//            NyttDokumentRiver(this, dokumentDao)
//            UtbetaltRiver(this, utbetaltDao, dokumentDao)
//            OldUtbetalingRiver(this, vedtakDao, dokumentDao)
//            TilUtbetalingBehovRiver(this, dokumentDao)
//            start()
        }
}

@KtorExperimentalAPI
internal fun Application.stonadsstatistikk(env: Environment.Auth, dokumentDao: DokumentDao, vedtakDao: VedtakDao) {
    azureAdAppAuthentication(env)
    install(ContentNegotiation) {
        jackson {
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            registerModule(JavaTimeModule())
        }
    }
    routing {
        authenticate {
            dokumenterApi(dokumentDao)
            grunnlagApi(vedtakDao)
        }
    }
}

internal fun Route.dokumenterApi(dokumentDao: DokumentDao) {
    get("/dokumenter") {
        val hendelseIder = call.request.queryParameters.getAll("hendelseId")
            ?.map { UUID.fromString(it) } ?: emptyList()
        val time = measureTimeMillis {
            try {
                call.respond(HttpStatusCode.OK, dokumentDao.finnHendelser(hendelseIder))
            } catch (e: Exception) {
                log.error("Feil ved oppslag av dokumenter", e)
                tjenestekallLog.error("Feil ved oppslag av dokumenter", e)
                call.respond(HttpStatusCode.InternalServerError, "Feil ved oppslag av dokumenter")
            }
        }
        tjenestekallLog.info("Hentet dokumenter for hendelser $hendelseIder ($time ms)")
    }
}

internal fun Route.grunnlagApi(vedtakDAO: VedtakDao) {
    get("/grunnlag") {
        val fødselsnummer = call.request.queryParameters["fodselsnummer"]
            ?: run {
                log.error("/grunnlag Mangler fodselsnummer query param")
                return@get call.respond(HttpStatusCode.BadRequest, "Mangler fodselsnummer query param")
            }
        val time = measureTimeMillis {
            try {
                call.respond(HttpStatusCode.OK, vedtakDAO.hentVedtakListe(fødselsnummer))
            } catch (e: Exception) {
                log.error("Feil ved henting av vedtak", e)
                tjenestekallLog.error("Feil ved henting av vedtak for $fødselsnummer", e)
                call.respond(HttpStatusCode.InternalServerError, "Feil ved henting av vedtak")
            }
        }
        tjenestekallLog.info("FP hentet vedtak for $fødselsnummer ($time ms)")
    }
}
