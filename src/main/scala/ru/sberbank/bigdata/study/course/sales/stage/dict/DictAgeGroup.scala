package ru.sberbank.bigdata.study.course.sales.stage.dict

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.IntegerType
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object DictAgeGroup extends SparkApp {
  override val name: String = "dict_age_group"

  /*
   * Справочник возрастной группы клиента
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(
        upper(trim(col("age_group_id"))).cast(IntegerType).as("age_group_id"),
        trim(col("age_group_name")).as("age_group_name")
      )
      .repartition(1)
  }
}
