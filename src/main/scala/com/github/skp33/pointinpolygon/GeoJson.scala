package com.github.skp33.pointinpolygon

import org.apache.spark.sql.{Encoders, SparkSession}

case class Polygons(coordinates: Array[Array[Array[Array[Double]]]], uniqueId: String)

object GeoJson {
  type PolygonMap = Map[String, List[(Double, Double)]]

  def getPolygonFromGeoJson(spark: SparkSession,
                            geoJsonFilePath: String,
                            uniqueId: String): PolygonMap = {

    import org.apache.spark.sql.functions._

    val geoData = spark.read.json(
      spark.createDataset(spark.sparkContext.wholeTextFiles(geoJsonFilePath).values)(Encoders.STRING)
    ).select(
      explode(col("features")).as("feature")
    ).filter("feature.geometry.type = 'MultiPolygon'")

    require(geoData.count > 0l, new IllegalArgumentException(
      """Except "MultiPolygon" no other type supported for feature.geometry.type""")
    )

    import spark.implicits._
    /**
     * There is a chance for uniqueId ie "GEOID10", can be multiple Polygons, so adding incremental
     * prefix with "." separator and can be removed once we identify the Polygon for uniqueId.
     */
    geoData.select(
      col("feature.geometry.coordinates").as("coordinates"),
      col("feature.properties." + uniqueId).as("uniqueId")
    ).as[Polygons].collect.flatMap(
      geo => geo.coordinates.flatten.map(_.map(y => (y(1), y(0))).toList).zipWithIndex.map(
        x => (x._1, x._2 + "." + geo.uniqueId)
      ).map(_.swap)
    ).toMap
  }
}
