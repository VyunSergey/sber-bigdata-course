package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Terminals extends SparkApp {
  override val name: String = "terminals"

  /*
   * Логика обработки датасета `Terminals` с терминалами,в которых клиенты совершают транзакции
   * Терминалы имеют следующие признаки:
   *  `tid` - уникальный id терминала
   *  `latitude` - широта географических координат расположения терминала
   *  `longitude` - долгота географических координат расположения терминала
   *  `internet_flag` - признак on-line терминала
   *  `category` - категория терминала
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    get(path = path.getParent.getParent.resolve("src").resolve(name))
      .select(
        upper(trim(col("tid"))).as("terminal_id"),
        trim(col("latitude")).cast(DecimalType(30, 15)).as("latitude"),
        trim(col("longitude")).cast(DecimalType(30, 15)).as("longitude"),
        trim(col("okrug_mt_num")).cast(IntegerType).as("okrug_mt_num"),
        trim(col("region_id")).cast(IntegerType).as("region_id"),
        trim(col("internet_flag")).cast(IntegerType).as("internet_flag"),
        trim(col("category")).cast(IntegerType).as("category")
      )
      .repartition(1)
  }
}
