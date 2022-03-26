package ru.sberbank.bigdata.study.course.sales.datamart

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, floor, hour, lit, max, row_number, sum, trim, upper, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp
import ru.sberbank.bigdata.study.course.sales.stage.dict.{DictAgeGroup, DictGender, DictTransCategory}
import ru.sberbank.bigdata.study.course.sales.stage.{Calendar, Clients, Terminals, Transactions}

import java.sql.Date

object SalesPoints extends SparkApp {
  override val name: String = "sales_points"
  override val partitionColName: Option[String] = Some("date")

  // TODO add descriptions to method `gen`
  /*
   *
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    val dictAgeGroup: DataFrame = DictAgeGroup.get()
    val dictGender: DataFrame = DictGender.get()
    val dictTransCategory: DataFrame = DictTransCategory.get()

    val calendar: DataFrame = Calendar.get()
    val clients: DataFrame = Clients.get()
    val terminals: DataFrame = Terminals.get()
    val transactions: DataFrame = Transactions.get()

    val calendarFiltered: DataFrame =
      calendar
        .filter(col("date").between(start, end))
        .select(
          col("date"),
          col("description").as("day_type")
        )

    val clientsWithAgeGender: DataFrame =
      clients.as("cli")
        .join(broadcast(dictAgeGroup).as("age"),
          col("cli.age_group_id") === col("age.age_group_id"), "inner")
        .join(broadcast(dictGender).as("gen"),
          col("cli.gender_id") === col("gen.gender_id"), "inner")
        .select(
          col("cli.client_id"),
          col("cli.gender_id"),
          col("gen.gender_name"),
          col("cli.age_group_id"),
          col("age.age_group_name"),
          col("cli.okrug_num"),
          col("cli.region_code")
        )

    val transactionsWithData: DataFrame =
      transactions.as("trn")
        .filter(col("trn.trans_dt").between(start, end))
        .join(broadcast(dictTransCategory).as("cat"),
          col("trn.trans_category_id") === col("cat.trans_category_id"), "inner")
        .join(broadcast(calendarFiltered).as("cal"),
          col("trn.trans_dt") === col("cal.date"), "inner")
        .join(broadcast(clientsWithAgeGender).as("cli"),
          col("trn.client_id") === col("cli.client_id"), "inner")
        .join(broadcast(terminals).as("ter"),
          col("trn.terminal_id") === col("ter.terminal_id"), "inner")
        .select(
          col("trn.trans_id"),
          col("trn.trans_dt"),
          col("trn.trans_time"),
          col("trn.trans_sum"),
          col("trn.trans_success"),
          col("trn.trans_category_id"),
          col("cat.trans_category_name"),
          col("trn.client_id"),
          col("cli.gender_id"),
          col("cli.gender_name"),
          col("cli.age_group_id"),
          col("cli.age_group_name"),
          col("trn.terminal_id"),
          col("ter.latitude"),
          col("ter.longitude"),
          col("ter.okrug_mt_num"),
          col("ter.region_id"),
          col("ter.internet_flag"),
          col("ter.category"),
          col("trn.day_of_week"),
          col("cal.day_type"),
          col("trn.country"),
          col("trn.return")
        )
        .withColumn("date", col("trans_dt"))
        .withColumn("trans_sum", trim(col("trans_sum")).cast(DecimalType(30, 15)))
        .withColumn("hour", hour(col("trans_time")).cast(IntegerType))
        .withColumn("country", upper(trim(col("country"))))
        .withColumn("latitude", trim(col("latitude")).cast(DecimalType(30, 15)))
        .withColumn("longitude", trim(col("longitude")).cast(DecimalType(30, 15)))
        .withColumn("latitude_square", (floor(col("latitude") * 100.0) / 100.0).cast(DecimalType(30, 2)))
        .withColumn("longitude_square", (floor(col("longitude") * 100.0) / 100.0).cast(DecimalType(30, 2)))
        .withColumn("holiday",
          when(upper(trim(col("day_type"))) === "ВЫХОДНОЙ",         1)
         .when(upper(trim(col("day_type"))) === "ПРАЗДНИЧНЫЙ ДЕНЬ", 1)
         .otherwise(0)
        )

    val salesPointsCountry: DataFrame =
      transactionsWithData
        .groupBy(
          col("terminal_id"),
          col("date"),
          col("country")
        )
        .agg(
          sum(lit(1)).as("trans_country_count")
        )
        .withColumn("country_row_num",
          row_number().over(
            Window.partitionBy(col("terminal_id"), col("date"))
              .orderBy(col("trans_country_count").desc_nulls_last)
          )
        )
        .filter(col("country_row_num") === 1)
        .select(
          col("terminal_id"),
          col("date"),
          col("country")
        )

    val salesPointsSumCount: DataFrame =
      transactionsWithData
        .groupBy(
          col("terminal_id"),
          col("date")
        )
        .agg(
          sum(col("trans_sum")).as("trans_sum"),
          sum(lit(1)).as("trans_count"),

          sum(when(upper(trim(col("gender_name"))) === "MALE",   col("trans_sum")).otherwise(0)).as("trans_sum_male"),
          sum(when(upper(trim(col("gender_name"))) === "FEMALE", col("trans_sum")).otherwise(0)).as("trans_sum_female"),

          sum(when(upper(trim(col("gender_name"))) === "MALE",   lit(1)).otherwise(0)).as("trans_count_male"),
          sum(when(upper(trim(col("gender_name"))) === "FEMALE", lit(1)).otherwise(0)).as("trans_count_female"),

          sum(when(upper(trim(col("age_group_name"))) === "<10",               col("trans_sum")).otherwise(0)).as("trans_sum_age_group_1"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 11 AND 20", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_2"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 21 AND 30", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_3"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 31 AND 40", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_4"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 41 AND 50", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_5"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 51 AND 60", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_6"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 61 AND 70", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_7"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 71 AND 80", col("trans_sum")).otherwise(0)).as("trans_sum_age_group_8"),
          sum(when(upper(trim(col("age_group_name"))) === ">80",               col("trans_sum")).otherwise(0)).as("trans_sum_age_group_9"),

          sum(when(upper(trim(col("age_group_name"))) === "<10",               lit(1)).otherwise(0)).as("trans_count_age_group_1"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 11 AND 20", lit(1)).otherwise(0)).as("trans_count_age_group_2"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 21 AND 30", lit(1)).otherwise(0)).as("trans_count_age_group_3"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 31 AND 40", lit(1)).otherwise(0)).as("trans_count_age_group_4"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 41 AND 50", lit(1)).otherwise(0)).as("trans_count_age_group_5"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 51 AND 60", lit(1)).otherwise(0)).as("trans_count_age_group_6"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 61 AND 70", lit(1)).otherwise(0)).as("trans_count_age_group_7"),
          sum(when(upper(trim(col("age_group_name"))) === "BETWEEN 71 AND 80", lit(1)).otherwise(0)).as("trans_count_age_group_8"),
          sum(when(upper(trim(col("age_group_name"))) === ">80",               lit(1)).otherwise(0)).as("trans_count_age_group_9"),

          sum(when(col("hour").between(6, 12),  col("trans_sum")).otherwise(0)).as("trans_sum_morning"),
          sum(when(col("hour").between(13, 18), col("trans_sum")).otherwise(0)).as("trans_sum_day"),
          sum(when(col("hour").between(19, 22), col("trans_sum")).otherwise(0)).as("trans_sum_evening"),
          sum(when(col("hour").between(23, 24), col("trans_sum"))
             .when(col("hour").between(0, 5),   col("trans_sum")).otherwise(0)).as("trans_sum_night"),

          sum(when(col("hour").between(6, 12),  lit(1)).otherwise(0)).as("trans_count_morning"),
          sum(when(col("hour").between(13, 18), lit(1)).otherwise(0)).as("trans_count_day"),
          sum(when(col("hour").between(19, 22), lit(1)).otherwise(0)).as("trans_count_evening"),
          sum(when(col("hour").between(23, 24), lit(1))
             .when(col("hour").between(0, 5),   lit(1)).otherwise(0)).as("trans_count_night"),

          sum(when(col("return") === "Y", col("trans_sum")).otherwise(0)).as("return_sum"),
          sum(when(col("return") === "Y", lit(1)).otherwise(0)).as("return_count"),

          max(col("day_of_week")).as("day_of_week"),

          max(col("latitude")).as("latitude"),
          max(col("longitude")).as("longitude"),

          max(col("latitude_square")).as("latitude_square"),
          max(col("longitude_square")).as("longitude_square"),

          max(col("okrug_mt_num")).as("okrug_mt_num"),
          max(col("region_id")).as("region_id"),
          max(col("internet_flag")).as("internet_flag"),
          max(col("category")).as("category"),
          max(col("holiday")).as("holiday")
        )

    salesPointsSumCount.as("sp1")
      .join(salesPointsCountry.as("sp2"),
        col("sp1.terminal_id") === col("sp2.terminal_id")
          && col("sp1.date") === col("sp2.date"), "inner")
      .select(
        col("sp1.terminal_id"),
        col("sp1.date"),

        col("sp1.trans_sum"),
        col("sp1.trans_count"),
        (col("sp1.trans_sum") / col("sp1.trans_count")).cast(DecimalType(30, 15)).as("trans_avg"),

        col("sp1.trans_sum_male"),
        col("sp1.trans_sum_female"),

        col("sp1.trans_count_male"),
        col("sp1.trans_count_female"),

        (col("sp1.trans_sum_male")   / col("sp1.trans_count_male")  ).cast(DecimalType(30, 15)).as("trans_avg_male"),
        (col("sp1.trans_sum_female") / col("sp1.trans_count_female")).cast(DecimalType(30, 15)).as("trans_avg_female"),

        col("sp1.trans_sum_age_group_1"),
        col("sp1.trans_sum_age_group_2"),
        col("sp1.trans_sum_age_group_3"),
        col("sp1.trans_sum_age_group_4"),
        col("sp1.trans_sum_age_group_5"),
        col("sp1.trans_sum_age_group_6"),
        col("sp1.trans_sum_age_group_7"),
        col("sp1.trans_sum_age_group_8"),
        col("sp1.trans_sum_age_group_9"),

        col("sp1.trans_count_age_group_1"),
        col("sp1.trans_count_age_group_2"),
        col("sp1.trans_count_age_group_3"),
        col("sp1.trans_count_age_group_4"),
        col("sp1.trans_count_age_group_5"),
        col("sp1.trans_count_age_group_6"),
        col("sp1.trans_count_age_group_7"),
        col("sp1.trans_count_age_group_8"),
        col("sp1.trans_count_age_group_9"),

        (col("sp1.trans_sum_age_group_1") / col("trans_count_age_group_1")).cast(DecimalType(30, 15)).as("trans_avg_age_group_1"),
        (col("sp1.trans_sum_age_group_2") / col("trans_count_age_group_2")).cast(DecimalType(30, 15)).as("trans_avg_age_group_2"),
        (col("sp1.trans_sum_age_group_3") / col("trans_count_age_group_3")).cast(DecimalType(30, 15)).as("trans_avg_age_group_3"),
        (col("sp1.trans_sum_age_group_4") / col("trans_count_age_group_4")).cast(DecimalType(30, 15)).as("trans_avg_age_group_4"),
        (col("sp1.trans_sum_age_group_5") / col("trans_count_age_group_5")).cast(DecimalType(30, 15)).as("trans_avg_age_group_5"),
        (col("sp1.trans_sum_age_group_6") / col("trans_count_age_group_6")).cast(DecimalType(30, 15)).as("trans_avg_age_group_6"),
        (col("sp1.trans_sum_age_group_7") / col("trans_count_age_group_7")).cast(DecimalType(30, 15)).as("trans_avg_age_group_7"),
        (col("sp1.trans_sum_age_group_8") / col("trans_count_age_group_8")).cast(DecimalType(30, 15)).as("trans_avg_age_group_8"),
        (col("sp1.trans_sum_age_group_9") / col("trans_count_age_group_9")).cast(DecimalType(30, 15)).as("trans_avg_age_group_9"),

        col("sp1.trans_sum_morning"),
        col("sp1.trans_sum_day"),
        col("sp1.trans_sum_evening"),
        col("sp1.trans_sum_night"),

        col("sp1.trans_count_morning"),
        col("sp1.trans_count_day"),
        col("sp1.trans_count_evening"),
        col("sp1.trans_count_night"),

        (col("sp1.trans_sum_morning") / col("trans_count_morning")).cast(DecimalType(30, 15)).as("trans_avg_morning"),
        (col("sp1.trans_sum_day")     / col("trans_count_day")    ).cast(DecimalType(30, 15)).as("trans_avg_day"),
        (col("sp1.trans_sum_evening") / col("trans_count_evening")).cast(DecimalType(30, 15)).as("trans_avg_evening"),
        (col("sp1.trans_sum_night")   / col("trans_count_night")  ).cast(DecimalType(30, 15)).as("trans_avg_night"),

        col("sp1.return_sum"),
        col("sp1.return_count"),

        (col("sp1.return_sum") / col("sp1.return_count")).cast(DecimalType(30, 15)).as("return_avg"),

        col("sp2.country"),
        col("sp1.day_of_week"),
        col("sp1.latitude"),
        col("sp1.longitude"),
        col("sp1.latitude_square"),
        col("sp1.longitude_square"),
        col("sp1.okrug_mt_num"),
        col("sp1.region_id"),
        col("sp1.internet_flag"),
        col("sp1.category"),
        col("sp1.holiday")
      )
      .repartition(col("date"))
  }
}
