package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class WarehouseTaskTest extends AnyFunSuite {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("WarehouseTask")
    .master("local[*]")
    .getOrCreate()

  test("simple test case") {
    val (response1, response2) = WarehouseTask.computeDataFrames(spark, "./src/test/resources/warehouse.csv", "./src/test/resources/amount.csv")

    response1.show
    response2.show

    val response1c = response1.collect
    assert(response1c(0).getAs[Double]("amount") == 13.73)
    assert(response1c(2).getAs[Double]("amount") == 12.44)
    assert(response1c(4).getAs[Double]("amount") == 51.87)
    assert(response1c(7).getAs[Double]("amount") == 19.75)
    assert(response1c(11).getAs[Double]("amount") == 70.14)
    assert(response1c(24).getAs[Double]("amount") == 80.73)
    assert(response1c(26).getAs[Double]("amount") == 53.68)
    assert(response1c(17).getAs[Double]("amount") == 13.97)
    assert(response1c(18).getAs[Double]("amount") == 55.59)
    assert(response1c(25).getAs[Double]("amount") == 88.63)
    assert(response1c(28).getAs[Double]("amount") == 70.31)
    assert(response1c(29).getAs[Double]("amount") == 83.68)

    val w1p2 = response2.filter(col("warehouse") === "W-1" && col("product") === "P-2").collect().head
    assert(w1p2.getAs[Double]("max_amount") == 57.58)
    assert(w1p2.getAs[Double]("min_amount") == 39.53)
    assert(w1p2.getAs[Double]("avg_amount") == 49.66)

    val w2p1 = response2.filter(col("warehouse") === "W-2" && col("product") === "P-1").collect().head
    assert(w2p1.getAs[Double]("max_amount") == 85.22)
    assert(w2p1.getAs[Double]("min_amount") == 19.75)
    assert(w2p1.getAs[Double]("avg_amount") == 52.49)

    val w4p2 = response2.filter(col("warehouse") === "W-4" && col("product") === "P-2").collect().head
    assert(w4p2.getAs[Double]("max_amount") == 86.59)
    assert(w4p2.getAs[Double]("min_amount") == 5.6)
    assert(w4p2.getAs[Double]("avg_amount") == 57.64)

    val w1p1 = response2.filter(col("warehouse") === "W-1" && col("product") === "P-1").collect().head
    assert(w1p1.getAs[Double]("max_amount") == 91.08)
    assert(w1p1.getAs[Double]("min_amount") == 13.73)
    assert(w1p1.getAs[Double]("avg_amount") == 52.41)

    val w7p3 = response2.filter(col("warehouse") === "W-7" && col("product") === "P-3").collect().head
    assert(w7p3.getAs[Double]("max_amount") == 83.68)
    assert(w7p3.getAs[Double]("min_amount") == 70.31)
    assert(w7p3.getAs[Double]("avg_amount") == 77.0)
  }

  test("missing amount columns") {
    val (response1, response2) = WarehouseTask.computeDataFrames(spark, "./src/test/resources/warehouse2.csv", "./src/test/resources/amount2.csv")

    assert(response1.filter(col("positionId") === 1).collect.head.getAs[Double]("amount") == 17.52)
    assert(response1.filter(col("positionId") === 2).collect.head.getAs[Double]("amount") == 57.13)
    assert(response1.filter(col("positionId") === 6).collect.head.getAs[Double]("amount") == 47.41)
    assert(response1.filter(col("positionId") === 11).collect.head.getAs[Double]("amount") == 58.02)
    assert(response1.filter(col("positionId") === 14).collect.head.getAs[Double]("amount") == 3.29)
    assert(response1.filter(col("positionId") === 16).collect.head.getAs[Double]("amount") == 98.42)
    assert(response1.filter(col("positionId") === 4).collect.head.getAs[Double]("amount") == 78.38)

    val w2p3 = response2.filter(col("warehouse") === "W-2" && col("product") === "P-3").collect().head
    assert(w2p3.getAs[Double]("max_amount") == 85.98)
    assert(w2p3.getAs[Double]("min_amount") == 51.99)
    assert(w2p3.getAs[Double]("avg_amount") == 72.12)

    val w2p2 = response2.filter(col("warehouse") === "W-2" && col("product") === "P-2").collect().head
    assert(w2p2.getAs[Double]("max_amount") == 94.07)
    assert(w2p2.getAs[Double]("min_amount") == 47.41)
    assert(w2p2.getAs[Double]("avg_amount") == 70.74)

    val w4p1 = response2.filter(col("warehouse") === "W-4" && col("product") === "P-1").collect().head
    assert(w4p1.getAs[Double]("max_amount") == 51.37)
    assert(w4p1.getAs[Double]("min_amount") == 14.54)
    assert(w4p1.getAs[Double]("avg_amount") == 32.96)

    val w4p3 = response2.filter(col("warehouse") === "W-4" && col("product") === "P-3").collect().head
    assert(w4p3.getAs[Double]("max_amount") == 58.02)
    assert(w4p3.getAs[Double]("min_amount") == 3.29)
    assert(w4p3.getAs[Double]("avg_amount") == 30.66)
  }
}
