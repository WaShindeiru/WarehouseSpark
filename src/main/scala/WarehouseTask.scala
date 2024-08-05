package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object MyUtils
{
  val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val unformatter = (x: String) => LocalDateTime.parse(x, formatter)
}

import MyUtils._

object WarehouseTask {

  case class Warehouse(positionId: Long, warehouse: String, product: String, eventTime: Timestamp)
  case class Amount(positionId: Long, amount: Double, eventTime: Timestamp)

  def computeDataFrames(spark: SparkSession, warehouseDF: DataFrame, amountDF: DataFrame): (DataFrame, DataFrame) = {
    import spark.implicits._

    val sparkUnformatter = udf((s: String) => unformatter(s))

    val warehouseDS = warehouseDF.withColumn("eventTime", sparkUnformatter(col("time"))).drop("time").as[Warehouse]
    val amountDS = amountDF.withColumn("eventTime", sparkUnformatter(col("time"))).drop("time").as[Amount]

    val recentAmount = amountDS.groupBy("positionId").agg(max("eventTime").as("eventTime"))

    val recentAmountDS = amountDS.as("a").join(broadcast(recentAmount.as("b")), $"a.eventTime" === $"b.eventTime" && $"b.positionId" === $"a.positionId")
      .select($"b.positionId", $"b.eventTime", $"a.amount")

    val result1 = warehouseDS.join(recentAmountDS, "positionId").select($"positionId", $"warehouse", $"product", $"amount")

    val result2 = result1.groupBy($"warehouse", $"product").agg(max("amount").alias("max_amount"),
        min("amount").alias("min_amount"), round(avg("amount"), 2).alias("avg_amount"))
      .select(col("warehouse"), col("product"), $"max_amount", $"min_amount", $"avg_amount")

    (result1, result2)
  }

  def computeDataFramesFromFile(spark: SparkSession, warehousePath: String = "./src/main/resources/warehouse.csv",
                        amountPath: String = "./src/main/resources/amount.csv"): (DataFrame, DataFrame) = {
    val warehouseSchema = new StructType()
      .add("positionId", LongType, nullable = false)
      .add("warehouse", StringType, nullable = false)
      .add("product", StringType, nullable = false)
      .add("time", StringType, nullable = false)

    val amountSchema = new StructType()
      .add("positionId", IntegerType, nullable = false)
      .add("amount", DoubleType, nullable = false)
      .add("time", StringType, nullable = false)

    val warehouseDF = spark.read
      .schema(warehouseSchema)
      .csv(warehousePath)

    val amountDF = spark.read
      .schema(amountSchema)
      .csv(amountPath)

    computeDataFrames(spark, warehouseDF, amountDF)
  }

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WarehouseTask")
      .master("local[*]")
      .getOrCreate()

    val sparkSession = spark.sessionState

    val (response1, response2) = computeDataFramesFromFile(spark)

    response1.show()
    response2.show()
  }

}

