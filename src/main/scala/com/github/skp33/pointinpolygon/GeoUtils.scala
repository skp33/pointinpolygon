package com.github.skp33.pointinpolygon

/**
 * Created by KPrajapati on 4/16/2018.
 */
object GeoUtils {
  // 1.1, 3.2, 3.3, 4.8, 6.8
  def findBound(list: Array[Double], target: Double): Array[Double] = {
    @annotation.tailrec
    def fb(list: Array[Double], start: Int, end: Int): Array[Double] = {
      if (start > end) return Array.empty[Double]
      val mid = start + (end - start + 1) / 2
      list match {
        case arr: Array[Double] if arr(mid) == target => Array(arr(mid))
        case arr: Array[Double] if arr(mid) > target =>
          val midl = mid - 1
          if (midl >= start) {
            if (arr(midl) < target) Array(arr(mid), arr(midl))
            else if (arr(midl) == target) Array(arr(midl))
            else fb(list, start, midl)
          }
          else {
            Array.empty[Double]
          }
        case arr: Array[Double] if arr(mid) < target =>
          val midr = mid + 1
          if (midr <= end) {
            if (arr(midr) > target) Array(arr(mid), arr(midr))
            else if (arr(midr) == target) Array(arr(midr))
            else fb(list, midr, end)
          }
          else {
            Array.empty[Double]
          }
      }
    }

    fb(list, 0, list.length - 1)
  }

  // assuming all lists are sorted
  def getIntersection(geoIdList: Array[String]*): Array[String] = {
    if (geoIdList.isEmpty) Array.empty[String]
    else geoIdList.reduceLeft(sortedIntersection)
  }

  def sortedIntersection(arr1: Array[String], arr2: Array[String]): Array[String] = {
    var i = 0
    var j = 0
    val m = arr1.length
    val n = arr2.length
    val intersect = collection.mutable.ListBuffer.empty[String]
    while (i < m && j < n) {
      if (arr1(i) < arr2(j)) {
        i += 1
      }
      else if (arr2(j) < arr1(i)) {
        j += 1
      }
      else {
        intersect += arr1(i)
        j += 1
        i += 1
      }
    }
    intersect.toArray
  }
}
