package ru.sberbank.bigdata.study.course.sales

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.study.course.sales.spark.SparkConnection

object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkConnection.createLocalSession()
    val srcPath: String = "C:/Users/Admin/Documents/PLEKHANOV/data/result/transactions"
    val tgtPath: String = "C:/Users/Admin/Documents/PLEKHANOV/data/test/grouped_transactions"

    spark.time {
      program(srcPath, tgtPath, spark)
    }
  }

  def program(srcPath: String, tgtPath: String, spark: SparkSession): Unit = {
    spark
      .read
      .format("csv")
      .options(Map("header" -> "true", "sep" -> ";", "encoding" -> "utf-8"))
      .load(srcPath)
      .repartition(col("trans_dt"))
      .groupBy("trans_dt", "tid")
      .agg(
        sum(lit(1)).as("count"),
        sum(col("trans_sum")).as("sum")
      )
      .write
      .format("csv")
      .mode("Overwrite")
      .partitionBy("trans_dt")
      .options(Map("header" -> "true", "sep" -> ";", "encoding" -> "utf-8"))
      .save(tgtPath)
  }
}
