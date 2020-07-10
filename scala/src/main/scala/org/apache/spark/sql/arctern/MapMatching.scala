/*
 * Copyright (C) 2019-2020 Zilliz. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.arctern

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.arctern.MapMatching.{compute, expandEnvelope, mapMatchingQuery}
import org.apache.spark.sql.arctern.expressions.ST_GeomFromWKB
import org.apache.spark.sql.arctern.functions._
import org.apache.spark.sql.{Column, DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.arctern.index.RTreeIndex
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, NumericType, StringType, StructField, StructType}
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}

object MapMatching extends Serializable {
  val defaultExpandValue = 100 // default expand value: 100 meters

  private def RAD2DEG(x: Double): Double = x * 180.0 / scala.math.Pi

  private def expandEnvelope(env: Envelope, expandValue: Double): Envelope = {
    val deg_distance = RAD2DEG(expandValue / 6371251.46)
    new Envelope(env.getMinX - deg_distance,
      env.getMaxX + deg_distance,
      env.getMinY - deg_distance,
      env.getMaxY + deg_distance)
  }

  private def envelopeCheck(env: Envelope): Boolean = env.getMinX > -90 && env.getMaxX < 90 && env.getMinY > -180 && env.getMaxY < 180

  private def mapMatchingQuery(point: Geometry, index: RTreeIndex): util.List[_] = {
    var ev = defaultExpandValue
    do {
      val env = expandEnvelope(point.getEnvelopeInternal, ev)
      if (!envelopeCheck(env)) return index.query(env)
      val rst = index.query(env)
      if (rst.size() > 0) return rst else ev *= 2
    } while (true)
    throw new Exception("Illegal operation in map matching query.")
  }

  private def projection(x: Double, y: Double, x1: Double, y1: Double, x2: Double, y2: Double): (Double, Double, Double) = {
    val L2 = (x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)
    if (L2 == 0.0) return (Double.MaxValue, -999999999, -999999999)
    val x1_x = x - x1
    val y1_y = y - y1
    val x1_x2 = x2 - x1
    val y1_y2 = y2 - y1
    var ratio = (x1_x * x1_x2 + y1_y * y1_y2) / L2
    ratio = if (ratio > 1) 1 else if (ratio < 0) 0 else ratio
    val prj_x = x1 + ratio * x1_x2
    val prj_y = y1 + ratio * y1_y2
    (scala.math.sqrt((x - prj_x) * (x - prj_x) + (y - prj_y) * (y - prj_y)), prj_x, prj_y)
  }

  private def compute(point: Geometry, road: Geometry): (Double, Double, Double) = {
    if (point.getGeometryType != "Point" || road.getGeometryType != "LineString") return (Double.MaxValue, -999999999, -999999999)
    val coordinates = road.getCoordinates
    val coordinate = point.getCoordinate
    var distance = Double.MaxValue
    var x: Double = -999999999
    var y: Double = -999999999
    for (i <- 0 until coordinates.size - 1) {
      val tmp = projection(coordinate.x, coordinate.y, coordinates(i).x, coordinates(i).y, coordinates(i + 1).x, coordinates(i + 1).y)
      if (tmp._1 <= distance) {
        distance = tmp._1
        x = tmp._2
        y = tmp._3
      }
    }
    (distance, x, y)
  }

  //  private def computeNearRoad(point: Geometry, index: Broadcast[RTreeIndex], expandValue: Double): Boolean = {
  //    val env = expandEnvelope(point.getEnvelopeInternal, expandValue)
  //    val results = index.value.query(env)
  //    results.size() > 0
  //  }

  private def buildIndex(roads: DataFrame): RTreeIndex = {
    val index = new RTreeIndex
    val idDataType = roads.col("roadsId").expr.dataType
    if (!(idDataType match {
      case _: NumericType => true
    })) throw new Exception("Unsupported index data type.")
    roads.collect().foreach {
      row => {
        val roadId = row.getAs[Int](0)
        val geo = row.getAs[Geometry](1)
        index.insert(geo.getEnvelopeInternal, roadId)
      }
    }
    index
  }

  def nearRoad(points: DataFrame, roads: DataFrame, pointsIndexColumn: Column, pointsColumn: Column, roadsIndexColumn: Column, roadsColumn: Column, expandValue: Double): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val thisPoints = points.select(pointsIndexColumn.as("pointsId"), pointsColumn.as("points"))
    val thisRoads = roads.select(roadsIndexColumn.as("roadsId"), roadsColumn.as("roads"))

    val index = buildIndex(thisRoads)
    val pointsDS = thisPoints.as[(Int, Geometry)]
    val roadsDS = thisRoads.as[(Int, Geometry)]
    val broadcast = spark.sparkContext.broadcast(index)

    val query = pointsDS.flatMap {
      tp => {
        val (pointId, point) = tp
        val rst = MapMatching.mapMatchingQuery(point, broadcast.value)
        rst.toArray.map(roadId => (roadId.asInstanceOf[Int], pointId, point))
      }
    }.withColumnRenamed("_1", "roadsId")
      .withColumnRenamed("_2", "pointsId")
      .withColumnRenamed("_3", "points")

    val joinQuery = query.join(roadsDS, "roadsId")
    joinQuery.show(false)

    // input: (Double, Geometry, Int, Geometry) => (distance, point, pointId, road)
    // return: Geometry => resultGeometry
    val computeCore = new Aggregator[(Int, Int, Geometry, Geometry), (Double, Geometry, Int, Geometry), String] {
      override def zero: (Double, Geometry, Int, Geometry) = (Double.MaxValue, null, -1, null)

      override def reduce(b: (Double, Geometry, Int, Geometry), a: (Int, Int, Geometry, Geometry)): (Double, Geometry, Int, Geometry) = {
        val point = a._3
        val road = a._4
        val rstProjection = MapMatching.compute(point, road)
        if (rstProjection._1 <= b._1) (rstProjection._1, a._3, a._2, a._4)
        else b
      }

      def merge(p1: (Double, Geometry, Int, Geometry), p2: (Double, Geometry, Int, Geometry)): (Double, Geometry, Int, Geometry) = {
        if (p1._1 <= p2._1) p1
        else p2
      }

      override def finish(reduction: (Double, Geometry, Int, Geometry)): String = reduction._4.toText

      override def bufferEncoder: Encoder[(Double, Geometry, Int, Geometry)] = implicitly[Encoder[(Double, Geometry, Int, Geometry)]]

//      override def outputEncoder: Encoder[GenericArrayData] = implicitly[Encoder[GenericArrayData]]
//      override def outputEncoder: Encoder[Geometry] = Encoders.product[GeometryUDT]
//      override def outputEncoder: Encoder[Geometry] = Encoders.kryo
      override def outputEncoder: Encoder[String] = implicitly[Encoder[String]]
    }

    val joinQueryDS = joinQuery.as[(Int, Int, Geometry, Geometry)]
    joinQueryDS.show(false)
    val rstDS = joinQueryDS.groupByKey(_._2).agg(computeCore.toColumn)
//    val rst = joinQueryDS.groupBy("pointsId").agg(computeCore.toColumn)
    println("************************************")
    rstDS.show(false)
    val rstDF = rstDS.toDF("pointsId", "nearestRoad")
    val rst = rstDF.select(col("pointsId"), st_geomfromtext(col("nearestRoad")))
    rst.show(false)
    rst
//    val rst = rstDF.select(col("pointsId"), ST_GeomFromWKB(Seq(col("nearestRoad").expr)))
  }

  //  def nearRoad(points: DataFrame, roads: DataFrame, pointsIndexColumn: Column, pointsColumn: Column, roadsIndexColumn: Column, roadsColumn: Column, expandValue: Double): DataFrame = {
  //    val spark = SparkSession.builder().getOrCreate()
  //
  //    import spark.implicits._
  //
  //    val thisPoints = points.select(pointsIndexColumn.as("pointsId"), pointsColumn.as("points"))
  //    val thisRoads = roads.select(roadsIndexColumn.as("roadsId"), roadsColumn.as("roads"))
  //
  //    val index = buildIndex(thisRoads)
  //    val pointsDS = thisPoints.as[(Int, Geometry)]
  //    val roadsDS = thisRoads.as[(Int, Geometry)]
  //    val broadcast = spark.sparkContext.broadcast(index)
  //
  //    val query = pointsDS.flatMap {
  //      tp => {
  //        val (pointId, point) = tp
  //        val rst = MapMatching.mapMatchingQuery(point, broadcast.value)
  //        rst.toArray.map(roadId => (roadId.asInstanceOf[Int], pointId, point))
  //      }
  //    }.withColumnRenamed("_1", "roadsId").withColumnRenamed("_2", "pointsId").withColumnRenamed("_3", "points")
  //
  //    val joinQuery = query.join(roadsDS, "roadsId")
  //    joinQuery.show(false)
  //
  //    // input: (Double, Geometry, Int, Geometry) => (distance, point, pointId, road)
  //    // return: (Int, Geometry) => (pointId, resultGeometry)
  //    val computeCore =  new Aggregator[(Int, Int, Geometry, Geometry), (Double, Geometry, Int, Geometry), (Int, Geometry)] {
  //      override def zero: (Double, Geometry, Int, Geometry) = (Double.MaxValue, null, -1, null)
  //
  //      override def reduce(b: (Double, Geometry, Int, Geometry), a: (Int, Int, Geometry, Geometry)): (Double, Geometry, Int, Geometry) = {
  //        val point = a._3
  //        val road = a._4
  //        val rstProjection = MapMatching.compute(point, road)
  //        if (rstProjection._1 <= b._1) (rstProjection._1, a._3, a._2, a._4)
  //        else b
  //      }
  //
  //      def merge(p1: (Double, Geometry, Int, Geometry), p2: (Double, Geometry, Int, Geometry)): (Double, Geometry, Int, Geometry) = {
  //        if (p1._1 <= p2._1) p1
  //        else p2
  //      }
  //
  //      override def finish(reduction: (Double, Geometry, Int, Geometry)): (Int, Geometry) = (reduction._3, reduction._4)
  //
  //      override def bufferEncoder: Encoder[(Double, Geometry, Int, Geometry)] = implicitly[Encoder[(Double, Geometry, Int, Geometry)]]
  //      override def outputEncoder: Encoder[(Int, Geometry)] = implicitly[Encoder[(Int, Geometry)]]
  //    }
  //
  //    val joinQueryDS = joinQuery.as[(Int, Int, Geometry, Geometry)]
  //    joinQueryDS.show(false)
  //    val rst = joinQueryDS.groupBy("pointsId").agg(computeCore.toColumn)
  //    println("************************************")
  //    rst.show(false)
  //    rst
  //  }

  private def computeNearestRoad(point: Geometry, index: Broadcast[RTreeIndex]): Geometry = {
    val results = mapMatchingQuery(point, index.value)
    if (results.size() <= 0) return new GeometryFactory().createLineString()
    var minDistance = Double.MaxValue
    var roadId: Int = -1
    for (i <- 0 until results.size()) {
      val road = results.get(i).asInstanceOf[Geometry]
      val rstProjection = compute(point, road)
      if (rstProjection._1 <= minDistance) {
        minDistance = rstProjection._1
        roadId = i
      }
    }
    results.get(roadId).asInstanceOf[Geometry]
  }

  private def computeNearestLocationOnRoad(point: Geometry, index: Broadcast[RTreeIndex]): Geometry = {
    val results = mapMatchingQuery(point, index.value)
    if (results.size() <= 0) return new GeometryFactory().createGeometryCollection()
    var minDistance = Double.MaxValue
    var x: Double = -999999999
    var y: Double = -999999999
    for (i <- 0 until results.size()) {
      val road = results.get(i).asInstanceOf[Geometry]
      val rstProjection = compute(point, road)
      if (rstProjection._1 <= minDistance) {
        minDistance = rstProjection._1
        x = rstProjection._2
        y = rstProjection._3
      }
    }
    new GeometryFactory().createPoint(new Coordinate(x, y))
  }
}

//class MapMatching {
//  private var roads: DataFrame = _
//
//  private var points: DataFrame = _
//
//  private val index: RTreeIndex = new RTreeIndex
//
//  private val spark = SparkSession.builder().getOrCreate()
//
//  private def setRoads(roads: DataFrame): Unit = this.roads = roads
//
//  private def setPoints(points: DataFrame): Unit = this.points = points
//
//  private def buildIndex(): Unit = {
//    val idDataType = roads.col("roadsId").expr.dataType
//    if (!(idDataType match { case _: NumericType => true })) throw new Exception("Unsupported index data type.")
//    roads.collect().foreach {
//      row => {
//        val roadId = row.getAs[Int](0)
//        val geo = row.getAs[Geometry](1)
//        index.insert(geo.getEnvelopeInternal, roadId)
//      }
//    }
//  }
//
//  def nearRoad(points: DataFrame, roads: DataFrame, pointsIndexColumn: Column, pointsColumn: Column, roadsIndexColumn: Column, roadsColumn: Column, expandValue: Double): DataFrame = {
//    import spark.implicits._
//
//    setPoints(points.select(pointsIndexColumn.as("pointsId"), pointsColumn.as("points")))
//    setRoads(roads.select(roadsIndexColumn.as("roadsId"), roadsColumn.as("roads")))
//
//    this.points.show(false)
//    this.roads.show(false)
//
//    buildIndex()
//    val pointsDS = this.points.as[(Int, Geometry)]
//    val roadsDS = this.roads.as[(Int, Geometry)]
//    val broadcast = spark.sparkContext.broadcast(index)
//
//    val query = pointsDS.flatMap {
//      tp => {
//        val (pointId, point) = tp
//        val rst = MapMatching.mapMatchingQuery(point, broadcast.value)
//        rst.toArray.map(roadId => (roadId.asInstanceOf[Int], pointId, point))
//      }
//    }.withColumnRenamed("_1", "roadsId").withColumnRenamed("_2", "pointsId").withColumnRenamed("_3", "points")
//
//    query.show(200, false)
//
//    val joinQuery = query.join(roadsDS, "roadsId")
//
//    // input: (Double, Geometry, Int, Geometry) => (distance, point, pointId, road)
//    // return: (Int, Geometry) => (pointId, resultGeometry)
//    val computeCore =  new Aggregator[(Int, Int, Geometry, Geometry), (Double, Geometry, Int, Geometry), (Int, Geometry)] {
//      override def zero: (Double, Geometry, Int, Geometry) = (Double.MaxValue, null, -1, null)
//
//      override def reduce(b: (Double, Geometry, Int, Geometry), a: (Int, Int, Geometry, Geometry)): (Double, Geometry, Int, Geometry) = {
//        val point = a._3
//        val road = a._4
//        val rstProjection = MapMatching.compute(point, road)
//        if (rstProjection._1 <= b._1) (rstProjection._1, a._3, a._2, a._4)
//        else b
//      }
//
//      def merge(p1: (Double, Geometry, Int, Geometry), p2: (Double, Geometry, Int, Geometry)): (Double, Geometry, Int, Geometry) = {
//        if (p1._1 <= p2._1) p1
//        else p2
//      }
//
//      override def finish(reduction: (Double, Geometry, Int, Geometry)): (Int, Geometry) = (reduction._3, reduction._4)
//
//      override def bufferEncoder: Encoder[(Double, Geometry, Int, Geometry)] = implicitly[Encoder[(Double, Geometry, Int, Geometry)]]
//      override def outputEncoder: Encoder[(Int, Geometry)] = implicitly[Encoder[(Int, Geometry)]]
//    }
//
//    joinQuery.show(200, false)
//    val rst = joinQuery.as[(Int, Int, Geometry, Geometry)].groupBy("pointsId").agg(computeCore.toColumn)
//    println("************************************")
//    rst.show(false)
//    rst
//  }
//
//  def nearestRoad(points: DataFrame, roads: DataFrame): DataFrame = {
//    setPoints(points)
//    setRoads(roads)
//    buildIndex()
//    val pointsRdd = points.rdd
//    val broadcast = spark.sparkContext.broadcast(index)
//    val rstRDD = pointsRdd.map(point => Row(MapMatching.computeNearestRoad(point.getAs[Geometry](0), broadcast)))
//    val rstSchema = StructType(Array(StructField("nearest_road", new GeometryUDT, nullable = false)))
//    spark.createDataFrame(rstRDD, rstSchema)
//  }
//
//  def nearestLocationOnRoad(points: DataFrame, roads: DataFrame): DataFrame = {
//    setPoints(points)
//    setRoads(roads)
//    buildIndex()
//    val pointsRdd = points.rdd
//    val broadcast = spark.sparkContext.broadcast(index)
//    val rstRDD = pointsRdd.map(point => Row(MapMatching.computeNearestLocationOnRoad(point.getAs[Geometry](0), broadcast)))
//    val rstSchema = StructType(Array(StructField("nearest_location_on_road", new GeometryUDT, nullable = false)))
//    spark.createDataFrame(rstRDD, rstSchema)
//  }
//}
