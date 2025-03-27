package opensky.models

case class AircraftState
(
    icao24: String,
    callsign: Option[String],
    originCountry: String,
    timePosition: Option[Long],
    lastContact: Long,
    longitude: Option[Double],
    latitude: Option[Double],
    altitude: Option[Double],
    onGround: Boolean,
    velocity: Option[Double],
    heading: Option[Double],
    verticalRate: Option[Double],
    sensors: Option[List[Int]],
    geoAltitude: Option[Double],
    squawk: Option[String],
    spi: Boolean,
    positionSource: Int
)
