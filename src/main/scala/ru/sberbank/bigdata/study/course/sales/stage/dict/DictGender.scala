package ru.sberbank.bigdata.study.course.sales.stage.dict

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.IntegerType
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object DictGender extends SparkApp {
  override val name: String = "dict_gender"

  /*
   * Справочник пола клиентов
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(
        upper(trim(col("gender_id"))).cast(IntegerType).as("gender_id"),
        trim(col("gender_name")).as("gender_name")
      )
      .repartition(1)
  }
}
