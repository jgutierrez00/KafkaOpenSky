package opensky.config

import com.typesafe.config.{Config, ConfigFactory}

trait AppConfig {

    protected val rootConfig: Config = ConfigFactory.load()

    val openskyConfig: Config = rootConfig.getConfig("opensky")
    val kafkaConfig: Config = rootConfig.getConfig("kafka")

    val openskyHost: String = openskyConfig.getString("url")
    val pollInterval: Long = openskyConfig.getDuration("poll-interval").toMillis
    val kafkaBootstrapServers: String = kafkaConfig.getString("bootstrap-servers")
    val kafkaTopic: String = kafkaConfig.getString("topic")
}
