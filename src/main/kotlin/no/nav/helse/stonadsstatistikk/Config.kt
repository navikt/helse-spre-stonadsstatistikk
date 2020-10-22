package no.nav.helse.stonadsstatistikk

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties

private val serviceuserBasePath = Paths.get("/var/run/secrets/nais.io/service_user")

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(serviceuserBasePath.resolve("username")),
    password = Files.readString(serviceuserBasePath.resolve("password"))
)

class Environment(
    val raw: Map<String, String>,
    val db: DB,
    val serviceUser: ServiceUser
) {
    constructor(raw: Map<String, String>) : this(
        raw = raw,
        db = DB(
            name = raw.getValue("DATABASE_NAME"),
            host = raw.getValue("DATABASE_HOST"),
            port = raw.getValue("DATABASE_PORT").toInt(),
            vaultMountPath = raw.getValue("DATABASE_VAULT_MOUNT_PATH")
        ),
        serviceUser = readServiceUserCredentials()
    )

    class DB(
        val name: String,
        val host: String,
        val port: Int,
        val vaultMountPath: String
    )
}

fun loadBaseConfig(kafkaBootstrapServers: String, serviceUser: ServiceUser): Properties = Properties().also {
    it[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
    it[SaslConfigs.SASL_MECHANISM] = "PLAIN"
    it[SaslConfigs.SASL_JAAS_CONFIG] = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
    it[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
}

fun Properties.toProducerConfig(): Properties = Properties().also {
    it.putAll(this)
    it[ProducerConfig.ACKS_CONFIG] = "all"
    it[ProducerConfig.CLIENT_ID_CONFIG] = "spre-stonadsstatistikk"
    it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
}

data class ServiceUser(
    val username: String,
    val password: String
)
