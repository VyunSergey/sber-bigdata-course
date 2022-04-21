package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types.DateType
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Calendar extends SparkApp {
  override val name: String = "calendar"
  override val partitionColName: Option[String] = Some("date")

  /*
   * Логика обработки датасета `Calendar` с разметкой календарных дней:
   *  `date` - календарный день
   *  `description` - категория дня
   *    РАБОЧИЙ ДЕНЬ
   *    ПРАЗДНИЧНЫЙ ДЕНЬ
   *    ВЫХОДНОЙ
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .filter(col("date").between(start, end))
      .select(
        trim(col("date")).cast(DateType).as("date"),
        trim(col("description")).as("description")
      )
      .repartition(col("date"))
  }
}
