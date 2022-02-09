package ru.sberbank.bigdata.study.course.sales.datamart

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object SalesLocations extends SparkApp {
  override val name: String = "sales_locations"
  override val partitionColumnNames: Seq[String] = Seq("date")

  /*
   *
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(

      )
      .repartition(col("date"))
  }
}
