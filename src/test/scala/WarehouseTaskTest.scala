package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class WarehouseTaskTest extends AnyFunSuite {

  val spark = SparkSession
    .builder
    .appName("WarehouseTask")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  Logger.getLogger("org").setLevel(Level.ERROR)

  import spark.implicits._

  test("test result1 result2, multiple amount for one position") {

    val testWarehouse = sc.parallelize(
      Seq(
        (1, "W-1", "P-2", "20240805104649"),
        (2, "W-1", "P-2", "20240805100649"),
        (3, "W-2", "P-2", "20240805102649"),
        (4, "W-2", "P-1", "20240805083649"),
        (5, "W-3", "P-2", "20240805095649"),
        (6, "W-3", "P-2", "20240805100649"))
    ).toDF("positionId", "warehouse", "product", "time")

    val testAmount = sc.parallelize(
      Seq(
        (1, 55.88, "20240805080642"),
        (1, 86.13, "20240805100647"),
        (1, 17.53, "20240805093723"),
        (2, 74.81, "20240805100709"),
        (2, 34.65, "20240805105721"),
        (3, 91.03, "20240805094625"),
        (4, 86.44, "20240805094656"),
        (5, 14.34, "20240805112705"),
        (5, 89.27, "20240805081625"),
        (5, 66.36, "20240805103701"),
        (6, 34.13, "20240805093630"),
        (6, 49.15, "20240805102652"))
    ).toDF("positionId", "amount", "time")

    println("Test warehouse DataFrame:")
    testWarehouse.show
    println("Test amount DataFrame:")
    testAmount.show

    val (result1, result2) = WarehouseTask.computeDataFrames(spark, testWarehouse, testAmount)

    val expectedResult1 = sc.parallelize(
      Seq(
        (1, "W-1", "P-2", 86.13),
        (2, "W-1", "P-2", 34.65),
        (3, "W-2", "P-2", 91.03),
        (4, "W-2", "P-1", 86.44),
        (5, "W-3", "P-2", 14.34),
        (6, "W-3", "P-2", 49.15))
    ).toDF("positionId", "warehouse", "product", "amount")

    val expectedResult2 = sc.parallelize(
      Seq(
        ("W-1", "P-2", 86.13, 34.65, 60.39),
        ("W-2", "P-1", 86.44, 86.44, 86.44),
        ("W-2", "P-2", 91.03, 91.03, 91.03),
        ("W-3", "P-2", 49.15, 14.34, 31.74))
    ).toDF("warehouse", "product", "max_amount", "min_amount", "avg_amount")

    println("Expected result1:")
    expectedResult1.show
    println("Expected result2:")
    expectedResult2.show

    assert(result1.collect sameElements expectedResult1.collect)
    assert(result2.collect sameElements expectedResult2.collect)
  }

  test("test result1 result2, missing amounts") {

    val testWarehouse = sc.parallelize(
      Seq(
        (1, "W-1", "P-2", "20240805132805"),
        (2, "W-1", "P-2", "20240805105805"),
        (3, "W-2", "P-1", "20240805115805"),
        (4, "W-3", "P-2", "20240805125805"),
        (5, "W-3", "P-2", "20240805101805"),
        (6, "W-4", "P-1", "20240805122805"))
    ).toDF("positionId", "warehouse", "product", "time")

    val testAmount = sc.parallelize(
      Seq(
        (1, 31.85, "20240805110755"),
        (1, 30.6, "20240805113749"),
        (1, 59.41, "20240805103759"),
        (3, 16.77, "20240805110818"),
        (4, 24.21, "20240805115742"),
        (5, 17.0, "20240805131743"),
        (5, 65.75, "20240805105836"))
    ).toDF("positionId", "amount", "time")

    println("Test warehouse DataFrame:")
    testWarehouse.show
    println("Test amount DataFrame:")
    testAmount.show

    val (result1, result2) = WarehouseTask.computeDataFrames(spark, testWarehouse, testAmount)

    val expectedResult1 = sc.parallelize(
      Seq(
        (1, "W-1", "P-2", 30.6),
        (3, "W-2", "P-1", 16.77),
        (4, "W-3", "P-2", 24.21),
        (5, "W-3", "P-2", 17.0))
    ).toDF("positionId", "warehouse", "product", "amount")

    val expectedResult2 = sc.parallelize(
      Seq(
        ("W-1", "P-2", 30.6, 30.6, 30.6),
        ("W-2", "P-1", 16.77, 16.77, 16.77),
        ("W-3", "P-2", 24.21, 17.0, 20.61))
    ).toDF("warehouse", "product", "max_amount", "min_amount", "avg_amount")

    println("Expected result1:")
    expectedResult1.show
    println("Expected result2:")
    expectedResult2.show

    assert(result1.collect sameElements expectedResult1.collect)
    assert(result2.collect sameElements expectedResult2.collect)
  }
}
