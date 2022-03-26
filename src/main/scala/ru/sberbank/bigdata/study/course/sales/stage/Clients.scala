package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Clients extends SparkApp {
  override val name: String = "clients"

  // TODO add descriptions to method `gen`
  /*
   *
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(
        upper(trim(col("cid"))).as("client_id"),
        upper(trim(col("gender"))).cast(IntegerType).as("gender_id"),
        upper(trim(col("age_group"))).cast(IntegerType).as("age_group_id"),
        trim(col("okrug_num")).cast(IntegerType).as("okrug_num"),
        trim(col("region_code")).cast(IntegerType).as("region_code")
      )
      .repartition(1)
  }
}
