package opensky.consumer

import opensky.config.AppConfig
import opensky.models.AircraftState
import org.apache.spark.sql._
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

import scala.concurrent.duration.DurationDouble
import scala.util.{Failure, Success, Try}

object SparkConsumer extends App with AppConfig{

    System.setProperty("hadoop.home.dir", "C:\\hadoop-3.4.1")
    System.load("C:\\hadoop-3.4.1\\bin\\hadoop.dll")

    val spark = SparkSession.builder
        .appName("opensky-consumer")
        .master("local[*]")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.hadoop.security.authorization", "false")
        .config("spark.sql.legacy.allowHadoopFileOverwrite", "true")
        .getOrCreate()


    import spark.implicits._

    val rawData = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServers)
        .option("subscribe", kafkaTopic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]

    implicit val formats: DefaultFormats = DefaultFormats

    val flightData = rawData.flatMap(parseWithRetry)

    val flightQuery = flightData.writeStream
        .outputMode("append")
        .format("console")
        .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
        .start()

    flightQuery.awaitTermination()

    private def parseJsonToState(json: String): Option[List[AircraftState]] = {

        implicit val formats: DefaultFormats = DefaultFormats

        try {
            val parsed = JsonMethods.parse(json)
            val states = (parsed \ "states").extractOpt[List[List[Any]]]

            states.map(_.map { arr: List[Any] =>
                AircraftState(
                    icao24 = arr(0).asInstanceOf[String],
                    callsign = Option(arr(1)).map(_.asInstanceOf[String]),
                    originCountry = arr(2).asInstanceOf[String],
                    timePosition = Option(arr(3)).map {
                        case bi: BigInt => bi.longValue()
                        case l: Long => l
                    },
                    lastContact = arr(4) match {
                        case bi: BigInt => bi.longValue()
                        case l: Long => l
                    },
                    longitude = Option(arr(5)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    latitude = Option(arr(6)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    altitude = Option(arr(7)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    onGround = arr(8).asInstanceOf[Boolean],
                    velocity = Option(arr(9)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    heading = Option(arr(10)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    verticalRate = Option(arr(11)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    sensors = Option(arr(12)).map {
                        case bi: BigInt => List(bi.intValue())
                        case l: List[Int] => l
                    },
                    geoAltitude = Option(arr(13)).map {
                        case bi: BigInt => bi.doubleValue()
                        case d: Double => d
                    },
                    squawk = Option(arr(14)).map(_.asInstanceOf[String]),
                    spi = arr(15).asInstanceOf[Boolean],
                    positionSource = arr(16) match {
                        case bi: BigInt => bi.intValue()
                        case i: Int => i
                    }
                )
            })
        } catch {
            case e: Exception =>
                println(s"Error parsing JSON: ${e.getMessage}")
                None
        }
    }

    private def parseWithRetry(jsonStr: String): List[AircraftState] = {
        var retryCount = 0
        val maxRetries = 5

        def retry(): List[AircraftState] = {
            Try {
                val json = JsonMethods.parse(jsonStr)
                val time = (json \ "time").extract[Long]
                val states = parseJsonToState(jsonStr)
                states.getOrElse(List.empty[AircraftState])
            } match {
                case Success(states) =>
                    retryCount = 0 // Reset retry count on success
                    states
                case Failure(ex) =>
                    if (ex.getMessage.contains("Too many requests")) {
                        retryCount += 1
                        if (retryCount <= maxRetries) {
                            val backoffTime = 10.seconds
                            println(s"Rate limit exceeded. Retrying in $backoffTime...")
                            Thread.sleep(backoffTime.toMillis)
                            retry()
                        } else {
                            println("Max retries reached. Skipping...")
                            List.empty[AircraftState]
                        }
                    } else {
                        println(s"Error parsing JSON: ${ex.getMessage}")
                        List.empty[AircraftState]
                    }
            }
        }

        retry()
    }
}