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
import org.apache.spark.sql.Row
import org.apache.spark.sql.arctern.GeometryUDT
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.arctern.functions._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader

class MapMatchingTest extends AdapterTest {
  val points = Seq(
    Row(1, new WKTReader().read("POINT (-73.961003 40.760594)")),
    Row(2, new WKTReader().read("POINT (-73.959908 40.776353)")),
    Row(3, new WKTReader().read("POINT (-73.955183 40.773459)")),
    Row(4, new WKTReader().read("POINT (-73.985233 40.744682)")),
    Row(5, new WKTReader().read("POINT (-73.997969 40.682816)")),
    Row(6, new WKTReader().read("POINT (-73.996458 40.758197)")),
    Row(7, new WKTReader().read("POINT (-73.988240 40.748960)")),
    Row(8, new WKTReader().read("POINT (-73.985185 40.735828)")),
    Row(9, new WKTReader().read("POINT (-73.989726 40.767795)")),
    Row(10, new WKTReader().read("POINT (-73.992669 40.768327)")),
  )

  val roads = Seq(
    Row(1, new WKTReader().read("LINESTRING (-73.9975944 40.7140611, -73.9974922 40.7139962)")),
    Row(2, new WKTReader().read("LINESTRING (-73.9980065 40.7138119, -73.9980743 40.7137811)")),
    Row(3, new WKTReader().read("LINESTRING (-73.9975554 40.7141073, -73.9975944 40.7140611)")),
    Row(4, new WKTReader().read("LINESTRING (-73.9978864 40.7143170, -73.9976740 40.7140968)")),
    Row(5, new WKTReader().read("LINESTRING (-73.9979810 40.7136728, -73.9980743 40.7137811)")),
    Row(6, new WKTReader().read("LINESTRING (-73.9980743 40.7137811, -73.9984728 40.7136003)")),
    Row(7, new WKTReader().read("LINESTRING (-73.9611014 40.7608112, -73.9610636 40.7608639)")),
    Row(8, new WKTReader().read("LINESTRING (-73.9594166 40.7593773, -73.9593736 40.7593593)")),
    Row(9, new WKTReader().read("LINESTRING (-73.9616090 40.7602969, -73.9615014 40.7602517)")),
    Row(10, new WKTReader().read("LINESTRING (-73.9615569 40.7601753, -73.9615014 40.7602517)")),
  )

  test("NearRoad") {
    val pointSchema = StructType(Array(StructField("pointsId", IntegerType, nullable = false), StructField("points", new GeometryUDT, nullable = false)))
    val roadSchema = StructType(Array(StructField("roadsId", IntegerType, nullable = false), StructField("roads", new GeometryUDT, nullable = false)))

    val pointsDF = spark.createDataFrame(spark.sparkContext.parallelize(points), pointSchema)
    val roadsDF = spark.createDataFrame(spark.sparkContext.parallelize(roads), roadSchema)

    val rst = near_road(pointsDF, roadsDF, pointsDF.col("pointsId"), pointsDF.col("points"), roadsDF.col("roadsId"), roadsDF.col("roads"), 1000)
    rst.show(false)

    val collect = rst.collect()
//    assert(collect(0).getAs[Boolean](0) == true)
//    assert(collect(1).getAs[Boolean](0) == false)
//    assert(collect(2).getAs[Boolean](0) == false)
//    assert(collect(3).getAs[Boolean](0) == false)
//    assert(collect(4).getAs[Boolean](0) == false)
//    assert(collect(5).getAs[Boolean](0) == false)
//    assert(collect(6).getAs[Boolean](0) == false)
//    assert(collect(7).getAs[Boolean](0) == false)
//    assert(collect(8).getAs[Boolean](0) == false)
//    assert(collect(9).getAs[Boolean](0) == false)
  }

  test("NearestRoad") {
    val pointSchema = StructType(Array(StructField("pointsId", IntegerType, nullable = false), StructField("points", new GeometryUDT, nullable = false)))
    val roadSchema = StructType(Array(StructField("roadsId", IntegerType, nullable = false), StructField("roads", new GeometryUDT, nullable = false)))

    val pointsDF = spark.createDataFrame(spark.sparkContext.parallelize(points), pointSchema)
    val roadsDF = spark.createDataFrame(spark.sparkContext.parallelize(roads), roadSchema)

    val rst = nearest_road(pointsDF, roadsDF, pointsDF.col("pointsId"), pointsDF.col("points"), roadsDF.col("roadsId"), roadsDF.col("roads"))
    rst.show(false)

    val collect = rst.collect()
//    assert(collect(0).getAs[Geometry](0).toText == "LINESTRING (-73.9611014 40.7608112, -73.9610636 40.7608639)")
//    assert(collect(1).getAs[Geometry](0).toText == "LINESTRING (-73.9611014 40.7608112, -73.9610636 40.7608639)")
//    assert(collect(2).getAs[Geometry](0).toText == "LINESTRING (-73.9611014 40.7608112, -73.9610636 40.7608639)")
//    assert(collect(3).getAs[Geometry](0).toText == "LINESTRING (-73.9615569 40.7601753, -73.9615014 40.7602517)")
//    assert(collect(4).getAs[Geometry](0).toText == "LINESTRING (-73.9980743 40.7137811, -73.9984728 40.7136003)")
//    assert(collect(5).getAs[Geometry](0).toText == "LINESTRING (-73.961609 40.7602969, -73.9615014 40.7602517)")
//    assert(collect(6).getAs[Geometry](0).toText == "LINESTRING (-73.961609 40.7602969, -73.9615014 40.7602517)")
//    assert(collect(7).getAs[Geometry](0).toText == "LINESTRING (-73.9978864 40.714317, -73.997674 40.7140968)")
//    assert(collect(8).getAs[Geometry](0).toText == "LINESTRING (-73.961609 40.7602969, -73.9615014 40.7602517)")
//    assert(collect(9).getAs[Geometry](0).toText == "LINESTRING (-73.961609 40.7602969, -73.9615014 40.7602517)")
  }

  test("NearestLocationOnRoad") {
    val pointSchema = StructType(Array(StructField("pointsId", IntegerType, nullable = false), StructField("points", new GeometryUDT, nullable = false)))
    val roadSchema = StructType(Array(StructField("roadsId", IntegerType, nullable = false), StructField("roads", new GeometryUDT, nullable = false)))

    val pointsDF = spark.createDataFrame(spark.sparkContext.parallelize(points), pointSchema)
    val roadsDF = spark.createDataFrame(spark.sparkContext.parallelize(roads), roadSchema)

    val rst = nearest_location_on_road(pointsDF, roadsDF, pointsDF.col("pointsId"), pointsDF.col("points"), roadsDF.col("roadsId"), roadsDF.col("roads"))
    rst.show(false)

    val collect = rst.collect()
//    assert(collect(0).getAs[Geometry](1).toText == "POINT (-73.9611014 40.7608112)")
//    assert(collect(1).getAs[Geometry](1).toText == "POINT (-73.9610636 40.7608639)")
//    assert(collect(2).getAs[Geometry](1).toText == "POINT (-73.9610636 40.7608639)")
//    assert(collect(3).getAs[Geometry](1).toText == "POINT (-73.9615569 40.7601753)")
//    assert(collect(4).getAs[Geometry](1).toText == "POINT (-73.9984728 40.7136003)")
//    assert(collect(5).getAs[Geometry](1).toText == "POINT (-73.961609 40.7602969)")
//    assert(collect(6).getAs[Geometry](1).toText == "POINT (-73.961609 40.7602969)")
//    assert(collect(7).getAs[Geometry](1).toText == "POINT (-73.9978864 40.714317)")
//    assert(collect(8).getAs[Geometry](1).toText == "POINT (-73.961609 40.7602969)")
//    assert(collect(9).getAs[Geometry](1).toText == "POINT (-73.961609 40.7602969)")
  }
}
