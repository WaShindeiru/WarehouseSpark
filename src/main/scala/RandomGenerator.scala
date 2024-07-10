package org.grid

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random
import org.apache.spark.sql._

object RandomGenerator {

  def main(args: Array[String]) = {
    val numberOfAmount = 1000
    val maxProductNumber: Int = 5
    val maxNumberOfWarehouse: Int = 10

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate
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

    import spark.implicits._

    val sequence1 = Seq.fill(numberOfAmount){warehouseGeneratorHelper()}
    val df1 = sc.parallelize(sequence1).toDF("positionId", "warehouse", "product", "time")


    val sequence2 = LazyList.from(1).take(numberOfAmount).flatMap(n => LazyList.fill(rand.between(1, 6))(n))
      .map(n => (
        n,
        Math.floor((rand.between(0, 101) + rand.nextDouble()) * 100) / 100,
        LocalDateTime.now().minusHours(2).minusMinutes(rand.between(-12, 12) * 10).minusSeconds(rand.between(-30, 30)).format(formatter)))

    val df2 = sc.parallelize(sequence2).toDF("positionId", "amount", "time")

    df1.write.csv("./warehouse")
    df2.write.csv("./amount")

    //    df1.select("positionId", "warehouse", "product", "eventTime").write.format("avro").save("./here.avro")
  }
}

