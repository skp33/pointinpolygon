package com.github.skp33.pointinpolygon

case class GeoPoint(lat: Double, lon: Double) {

  /**
   * Check whether point is inside the polygon
   */
  def isIn(polygon: List[GeoPoint]): Boolean = {
    val vertex :: vertices = polygon.reverse
    polygon.zip({
      vertices :+ vertex
    }.reverse).foldLeft(false) { case (isIn, (pi, pj)) =>
      if (((pi.lon > lon) != (pj.lon > lon)) &&
        lat < (pj.lat - pi.lat) * (lon - pi.lon) / (pj.lon - pi.lon) + pi.lat) !isIn else isIn
    }
  }

  /**
   * Check whether point is inside list of polygons
   */
  @scala.annotation.tailrec
  final def isInAny(polygons: List[(String, List[GeoPoint])]): String = {
    polygons match {
      case head :: tail => if (isIn(head._2)) head._1 else isInAny(tail)
      case Nil => ""
    }
  }
}
