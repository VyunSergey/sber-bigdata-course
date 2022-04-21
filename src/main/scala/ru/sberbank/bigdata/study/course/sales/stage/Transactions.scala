package ru.sberbank.bigdata.study.course.sales.stage

import org.apache.spark.sql.functions.{col, trim, upper}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object Transactions extends SparkApp {
  override val name: String = "transactions"
  override val partitionColName: Option[String] = Some("trans_dt")

  /*
   * Логика обработки датасета `Transactions` с транзакциями, которые совершают клиенты в терминалах
   * Транзакции имеют следующие признаки:
   *  `trans_id` - уникальный id транзакции
   *  `trans_dt` - дата совершения транзакции
   *  `trans_time` - время совершения транзакции
   *  `trans_sum` - сумма транзакции
   *  `trans_success` - признак успешно совершенной транзакции
   *  `category` - категория транзакции
   *    1 - Всё для дома
   *    2 - Всё остальное
   *    3 - Денежные переводы
   *    4 - Дети и животные
   *    5 - Интернет-магазины
   *    6 - Личный транспорт
   *    7 - Медицина и косметика
   *    8 - Общепит
   *    9 - Одежда, обувь и аксессуары
   *   10 - Оплата счетов
   *   11 - Поездки, доставка, хранение
   *   12 - Супермаркеты и продуктовые магазины
   *   13 - Телефония и интернет
   *  `cid` - уникальный id клиента
   *  `tid` - уникальный id терминала
   *  `day_of_week` - день недели у даты совершения транзакции
   *  `country` - страна совершения транзакции
   *  `return` - признак возврата денег по транзакции
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
