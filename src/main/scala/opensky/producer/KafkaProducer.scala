package opensky.producer

import opensky.config.AppConfig
import org.apache.http.client.methods._
import org.apache.http.impl.client._
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer._
import java.util.Properties

object KafkaProducer extends App with AppConfig{

    private val producerProps = new Properties()

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2097152")

    private val producer = new KafkaProducer[String, String](producerProps)
    private val httpClient = HttpClientBuilder.create().build()

    def fetchData(retries: Int = 3, delay: Long = 1000): String = {
        var attempt = 0
        var data = ""

        while (attempt < retries && data.isEmpty) {
            try {
                val request = new HttpGet(openskyHost)
                val response = httpClient.execute(request)

                if (response.getStatusLine.getStatusCode == 200) {
                    val jsonResponse = EntityUtils.toString(response.getEntity)

                    if (jsonResponse.contains("\"states\"")) {
                        data = jsonResponse
                    } else {
                        println("Response does not contain flight data")
                    }
                } else if (response.getStatusLine.getStatusCode == 429) {
                    println("Too many requests, retrying...")
                    Thread.sleep(delay * (attempt + 1))
                } else {
                    println(s"HTTP error: ${response.getStatusLine.getStatusCode}")
                }
            } catch {
                case e: Exception => println(s"Error while fetching data: ${e.getMessage}")
            }
            attempt += 1
        }
        data
    }

    scala.sys.addShutdownHook {
        println("Shutting down Kafka producer")
        producer.close()
        httpClient.close()
    }

    while(true) {
        try {
            val data = fetchData()
            if (data.nonEmpty) {
                val record = new ProducerRecord[String, String](kafkaTopic, data)
                producer.send(record, new Callback {
                    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                        if (exception != null) {
                            println(s"Error sending record: ${exception.getMessage}")
                        } else {
                            println(s"Record sent to topic ${metadata.topic()} partition ${metadata.partition()} offset ${metadata.offset()}")
                        }
                    }
                })
                println(s"Sent data to Kafka: ${data.take(100)}...")
            }
        } catch {
            case e: Exception => println(s"Error: ${e.getMessage}")
        }

        Thread.sleep(pollInterval)
    }
}