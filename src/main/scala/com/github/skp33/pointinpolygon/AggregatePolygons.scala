package com.github.skp33.pointinpolygon

import java.io.{FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.github.skp33.pointinpolygon.GeoJson.PolygonMap
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

case class AggregatePolygons(lat: Array[Double],
                             lon: Array[Double],
                             latWithId: Map[Double, Array[String]],
                             lonWithId: Map[Double, Array[String]],
                             geoPolygons: PolygonMap) {

  def write(path: String)(implicit spark: SparkSession): Unit = {
    val kryo = new KryoSerializer(spark.sparkContext.getConf).newKryo()
    kryo.register(classOf[AggregatePolygons])
    val output = new Output(new FileOutputStream(path))
    kryo.writeObject(output, this)
    output.close()
  }

  def finalGeoId(isNearest: Boolean)(lat: Double, lon: Double): String = {
    val geoIds = filterGeoId(lat, lon).map(
      x => (x, this.geoPolygons(x).map(y => GeoPoint(y._1, y._2)))
    )

    val geoId = GeoPoint(lat, lon).isInAny(geoIds.toList) match {
      case "" if geoIds.length > 0 && isNearest => geoIds(0)._1
      case zip@_ => zip
    }
    geoId.substring(geoId.indexOf(".") + 1)
  }

  def filterGeoId(lat: Double, lon: Double): Array[String] = {
    GeoUtils.getIntersection(
      GeoUtils.findBound(this.lat, lat).map(this.latWithId).union(
        GeoUtils.findBound(this.lon, lon).map(this.lonWithId)
      ): _*
    )
  }
}

object AggregatePolygons {

  def read(path: String)(implicit spark: SparkSession): AggregatePolygons = {
    val kryo = new KryoSerializer(spark.sparkContext.getConf).newKryo()
    kryo.register(classOf[AggregatePolygons])
    val input = new Input(new FileInputStream(path))
    val someObject = kryo.readObject(input, classOf[AggregatePolygons])
    input.close()
    someObject
  }

  def apply(geoData: PolygonMap): AggregatePolygons = {
    val geoPolygonWithMMLL = addMinMaxLatLon(geoData)
    val allLatsWithGeoIds = getAggrigatedMap(geoPolygonWithMMLL.map(x => Bound(x.geoId, x.minLat, x.maxLat)))
    val allLat = allLatsWithGeoIds.keySet.toArray.sorted
    val allLonsWithGeoIds = getAggrigatedMap(geoPolygonWithMMLL.map(x => Bound(x.geoId, x.minLon, x.maxLon)))
    val allLon = allLonsWithGeoIds.keySet.toArray.sorted
    AggregatePolygons(allLat, allLon, allLatsWithGeoIds, allLonsWithGeoIds, geoData)
  }

  def addMinMaxLatLon(geoPolygons: PolygonMap): Array[PolygonWithMMLL] = {
    geoPolygons.map(x => {
      val minMax = findMinMax(x._2)
      PolygonWithMMLL(x._1, minMax._1, minMax._2, minMax._3, minMax._4, x._2)
    }).toArray
  }

  def findMinMax(ppoints: List[(Double, Double)]): (Double, Double, Double, Double) = {
    ppoints.foldLeft((200.0, -200.0, 200.0, -200.0)) {
      (r, point) =>
        (Math.min(r._1, point._1), Math.max(r._2, point._1),
          Math.min(r._3, point._2), Math.max(r._4, point._2))
    }
  }

  def getAggrigatedMap(bound: Array[Bound]): Map[Double, Array[String]] = {
    val allBoundsWithGeoIds = scala.collection.mutable.Map.empty[Double, scala.collection.SortedSet[String]]
    bound.flatMap(x => List(x.lowerBound, x.upperBound)).foreach(allBoundsWithGeoIds(_) = scala.collection.SortedSet.empty[String])
    bound.foreach(x => updateGeoIds(allBoundsWithGeoIds, x.lowerBound, x.upperBound, x.geoId))
    allBoundsWithGeoIds.mapValues(_.toArray).toMap
  }

  def updateGeoIds(emptyIds: scala.collection.mutable.Map[Double, scala.collection.SortedSet[String]],
                   min: Double, max: Double, geoId: String): Unit = {
    emptyIds.keys.filter(x => x >= min && x <= max).foreach(x => emptyIds(x) = emptyIds(x) + geoId)
  }

  case class PolygonWithMMLL(geoId: String, minLat: Double, maxLat: Double,
                             minLon: Double, maxLon: Double, polygon: List[(Double, Double)])

  case class Bound(geoId: String, lowerBound: Double, upperBound: Double)

}
