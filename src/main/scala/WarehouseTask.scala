package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}

import java.sql.Timestamp
import java.sql.Date
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object MyUtils
{
  val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val unformatter = (x: String) => LocalDateTime.parse(x, formatter)
}

import MyUtils._

object WarehouseTask {

  case class Warehouse(positionId: Int, warehouse: String, product: String, eventTime: Timestamp)
  case class Amount(positionId: Int, amount: Double, eventTime: Timestamp)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WarehouseTask")
      .master("local[*]")
      .getOrCreate()

    val sparkSession = spark.sessionState

    val warehouseSchema = new StructType()
      .add("positionId", IntegerType, nullable = false)
      .add("warehouse", StringType, nullable = false)
      .add("product", StringType, nullable = false)
      .add("time", StringType, nullable = false)

    val amountSchema = new StructType()
      .add("positionId", IntegerType, nullable = false)
      .add("amount", DoubleType, nullable = false)
      .add("time", StringType, nullable = false)

    import spark.implicits._
    val warehouseDF = spark.read
      .schema(warehouseSchema)
      .csv("./src/main/resources/warehouse.csv")

    val amountDF = spark.read
      .schema(amountSchema)
      .csv("./src/main/resources/amount.csv")

    val sparkUnformatter = udf((s: String) => unformatter(s))

    val warehouseDS = warehouseDF.withColumn("eventTime", sparkUnformatter(col("time"))).drop("time").as[Warehouse]
    val amountDS = amountDF.withColumn("eventTime", sparkUnformatter(col("time"))).drop("time").as[Amount]

    val recentAmount = amountDS.groupBy("positionId").agg(max("eventTime").as("eventTime"))
    // change the position
    // different join, broadcast join
    val recentAmountDS = amountDS.as("a").join(broadcast(recentAmount.as("b")), $"a.eventTime" === $"b.eventTime" && $"b.positionId" === $"a.positionId")
      .select($"b.positionId", $"b.eventTime", $"a.amount")

    // dataset with more records on the left
    val response1 = warehouseDS.join(recentAmountDS, "positionId").select($"positionId", $"warehouse", $"product", $"amount")

    val response2 = response1.groupBy($"warehouse", $"product").agg(max("amount").alias("max_amount"),
      min("amount").alias("min_amount"), avg("amount").alias("avg_amount"))
      .select(col("warehouse"), col("product"), $"max_amount", $"min_amount", $"avg_amount")

    response1.show()
    response2.show()
  }

}

