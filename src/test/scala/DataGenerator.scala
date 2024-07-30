package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object DataGenerator {

  def generateSampleData(spark: SparkSession, numRows: Int = 100, maxProductNumber: Int = 4, maxNumberOfWarehouse: Int = 8): Tuple2[DataFrame, DataFrame] = {
    
    import spark.implicits._
    val sc = spark.sparkContext

    val rand = new Random()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    val indexGenerator = LazyList.from(1)
    val indexIterator = indexGenerator.iterator

    val duplicatedList = indexGenerator.flatMap{n => LazyList.fill(rand.between(1, maxNumberOfWarehouse))(n)}
    val numberIterator = duplicatedList.iterator

    def warehouseGeneratorHelper(): Tuple4[Int, String, String, String] = {
      val index = indexIterator.next
      val j = numberIterator.next
      val productNumber = rand.between(1, maxProductNumber)
      (index, s"W-$j", s"P-$productNumber", LocalDateTime.now().minusHours(2).minusMinutes(rand.between(-12, 12) * 10).format(formatter))
    }
    
    val sequence1 = Seq.fill(numRows){warehouseGeneratorHelper()}
    val df1 = sc.parallelize(sequence1).toDF("positionId", "warehouse", "product", "time")


    val sequence2 = LazyList.from(1).take(numRows).flatMap(n => LazyList.fill(rand.between(1, 6))(n))
      .map{n => (
        n,
        Math.floor((rand.between(0, 101) + rand.nextDouble()) * 100) / 100,
        LocalDateTime.now().minusHours(2).minusMinutes(rand.between(-12, 12) * 10).minusSeconds(rand.between(-30, 30)).format(formatter))}

    val df2 = sc.parallelize(sequence2).toDF("positionId", "amount", "time")

    (df1, df2)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate

    val (df1, df2) = generateSampleData(spark, 16)

    df1.write.mode("overwrite").csv("./warehouse")
    df2.write.mode("overwrite").csv("./amount")
  }
}
