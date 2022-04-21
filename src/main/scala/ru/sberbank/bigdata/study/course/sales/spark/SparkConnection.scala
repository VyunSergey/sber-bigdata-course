package ru.sberbank.bigdata.study.course.sales.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConnection {

  /*
   * Спарк сессия необходимая для работы с данными через Apache Spark
   * */
  def createLocalSession(numCores: Option[Int] = None,
                         appName: String = "spark-session"): SparkSession = {
    val settings: Map[String, String] = Map(
      "spark.driver.cores" -> "1",
      "spark.driver.memory" -> "4g",
      "spark.driver.maxResultSize" -> "1g",
      "spark.executor.instances" -> "1",
      "spark.executor.memory" -> "4g",
      "spark.default.parallelism" -> "100",
      "spark.sql.shuffle.partitions" -> "100"
    ) ++ numCores.map(num => Map("spark.executor.cores" -> s"$num"))
      .getOrElse(Map.empty[String, String])

    val sparkConfig: SparkConf = new SparkConf(true)
      .setAll(settings)

    SparkSession.builder
      .master(s"local[${numCores.map(_.toString).getOrElse("*")}]")
      .appName(appName)
      .config(sparkConfig)
      .getOrCreate
  }

  object implicits {
    implicit lazy val spark: SparkSession = createLocalSession()
  }
}
