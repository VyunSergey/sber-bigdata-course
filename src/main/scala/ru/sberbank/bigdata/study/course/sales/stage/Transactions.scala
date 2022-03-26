package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Transactions extends SparkApp {
  override val name: String = "transactions"
  override val partitionColName: Option[String] = Some("trans_dt")

  // TODO add descriptions to method `gen`
  /*
   *
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .filter(col("trans_dt").between(start, end))
      .select(
        upper(trim(col("trans_id"))).as("trans_id"),
        trim(col("trans_dt")).cast(DateType).as("trans_dt"),
        trim(col("trans_time")).as("trans_time"),
        trim(col("trans_sum")).cast(DecimalType(30, 15)).as("trans_sum"),
        upper(trim(col("trans_success"))).as("trans_success"),
        upper(trim(col("category"))).cast(IntegerType).as("trans_category_id"),
        upper(trim(col("cid"))).as("client_id"),
        upper(trim(col("tid"))).as("terminal_id"),
        upper(trim(col("day_of_week"))).as("day_of_week"),
        upper(trim(col("country"))).as("country"),
        upper(trim(col("return"))).as("return")
      )
      .repartition(col("trans_dt"))
  }
}
