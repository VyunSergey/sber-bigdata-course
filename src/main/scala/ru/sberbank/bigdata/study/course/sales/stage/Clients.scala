package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Clients extends SparkApp {
  override val name: String = "clients"

  /*
   * Логика обработки датасета `Clients` с клиентами, которые совершали транзакции
   * Клиенты имеют следующие признаки:
   *  `cid` - уникальный id клиента
   *  `gender` - пол
   *    1 - мужчина
   *    2 - женщина
   *  `age_group` - возрастная группа
   *    1 - до 10 лет
   *    2 - от 11 до 20 лет
   *    3 - от 21 до 30 лет
   *    4 - от 31 до 40 лет
   *    5 - от 41 до 50 лет
   *    6 - от 51 до 60 лет
   *    7 - от 61 до 70 лет
   *    8 - от 71 до 80 лет
   *    9 - от 81 года
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
