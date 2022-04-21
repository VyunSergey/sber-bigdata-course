package ru.sberbank.bigdata.study.course.sales.stage.dict

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.IntegerType
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object DictTransCategory extends SparkApp {
  override val name: String = "dict_trans_category"

  /*
   * Справочник категорий транзакций
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(
        upper(trim(col("category_id"))).cast(IntegerType).as("trans_category_id"),
        trim(col("category_name")).as("trans_category_name")
      )
      .repartition(1)
  }
}
