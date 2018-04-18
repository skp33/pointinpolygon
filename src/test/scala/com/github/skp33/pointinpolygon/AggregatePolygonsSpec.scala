package com.github.skp33.pointinpolygon

import com.github.skp33.pointinpolygon.GeoJson.PolygonMap
import org.apache.spark.sql.SparkSession
import org.scalatest._

class AggregatePolygonsSpec extends FunSuite with Matchers {
  test("point should map to a zip") {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("pointinpolygon")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val geojson: PolygonMap = GeoJson.getPolygonFromGeoJson(
      spark, getClass.getResource("/uszip-sample.json").toString, "GEOID10")

    val polygons = AggregatePolygons(geojson)

    // if point not found in any polygon it gives nearest polygon
    val isNearest = true

    val features = polygons.finalGeoId(isNearest)(43.442629, -83.107333)
    features shouldEqual "48453"
  }

  test("Able to read and write aggregated polygon data") {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("pointinpolygon")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val geojson: PolygonMap = GeoJson.getPolygonFromGeoJson(
      spark, getClass.getResource("/uszip-sample.json").toString, "GEOID10")

    val polygons = AggregatePolygons(geojson)

    // if point not found in any polygon it gives nearest polygon
    val isNearest = true

    // Save AggregatePolygons to file
    polygons.write("polygon.dat")

    // read AggregatePolygons from file
    val _polygons = AggregatePolygons.read("polygon.dat")

    val features = polygons.finalGeoId(isNearest)(43.442629, -83.107333)
    features shouldEqual "48453"
  }

  test("Api should work with spark DataFrame") {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("pointinpolygon")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val geojson: PolygonMap = GeoJson.getPolygonFromGeoJson(
      spark, getClass.getResource("/uszip-sample.json").toString, "GEOID10")

    val polygons = AggregatePolygons(geojson)

    // if point not found in any polygon it gives nearest polygon
    val isNearest = true

    val _polygons = spark.sparkContext.broadcast(polygons)
    val points = Seq(
      (43.220428, -83.471009),
      (42.953935, -83.925776),
      (43.096775, -83.732236),
      (43.416791, -83.996291),
      (43.608231, -84.148488))

    import spark.implicits._
    val pointsDF = spark.createDataset(points).toDF("lat", "lon")

    val pointWithZip = pointsDF.map(point =>
      _polygons.value.finalGeoId(isNearest)(point.getAs[Double]("lat"), point.getAs[Double]("lon"))
    )
    pointWithZip.collect.sorted  shouldEqual Array("48458", "48463", "48473", "48638", "48642")
  }
}
