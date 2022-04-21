package ru.sberbank.bigdata.study.course.sales.datamart

import org.apache.spark.sql.functions.{abs, coalesce, col, lit, max, sum, trim}
import org.apache.spark.sql.types.{DecimalType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.bigdata.study.course.sales.spark.SparkApp

import java.sql.Date

object SalesLocations extends SparkApp {
  override val name: String = "sales_locations"
  override val partitionColName: Option[String] = Some("date")

  /*
   * Логика расчета датасета `SalesLocations` к которому добавляются дополнительные признаки от датасета `SalesPoints`
   * сумма, количество и средняя сумма транзакций в разрезе квадрата со всеми терминалами, полученными округлением координат 2 знаках после плавающей точки
   * сумма, количество и средняя сумма транзакций в разрезе всех соседних квадратов относительно данного
   * */
  override def gen(start: Date, end: Date)(implicit spark: SparkSession): DataFrame = {
    val salesPoints: DataFrame = SalesPoints.get()

    val salesPointsFiltered: DataFrame =
      salesPoints
        .filter(col("date").between(start, end))
        .withColumn("trans_count", trim(col("trans_count")).cast(LongType))
        .withColumn("trans_sum", trim(col("trans_sum")).cast(DecimalType(30, 15)))
        .withColumn("latitude_square", trim(col("latitude_square")).cast(DecimalType(30, 15)))
        .withColumn("longitude_square", trim(col("longitude_square")).cast(DecimalType(30, 15)))

    val squarePoints: DataFrame =
      salesPointsFiltered
        .groupBy(
          col("date"),
          col("latitude_square"),
          col("longitude_square")
        )
        .agg(
          sum(col("trans_sum")).as("square_sum"),
          sum(col("trans_count")).as("square_count")
        )
        .select(
          col("date"),
          col("latitude_square"),
          col("longitude_square"),
          col("square_sum"),
          col("square_count"),
          (col("square_sum") / col("square_count")).cast(DecimalType(30, 15)).as("square_avg")
        )
        .cache()

    val squarePointsWithNeighbours: DataFrame =
      squarePoints.as("sq1")
        .join(squarePoints.as("sq2"),
          col("sq1.date") === col("sq2.date") &&
            abs(col("sq1.latitude_square") - col("sq2.latitude_square")) <= 0.01 &&
            abs(col("sq1.longitude_square") - col("sq2.longitude_square")) <= 0.01 &&
            (abs(col("sq1.latitude_square") - col("sq2.latitude_square")) +
              abs(col("sq1.longitude_square") - col("sq2.longitude_square"))) > 0.0, "left")
        .select(
          col("sq1.date"),
          col("sq1.latitude_square"),
          col("sq1.longitude_square"),
          col("sq1.square_sum"),
          col("sq1.square_count"),
          col("sq1.square_avg"),
          col("sq2.latitude_square").as("neighbour_latitude"),
          col("sq2.longitude_square").as("neighbour_longitude"),
          coalesce(col("sq2.square_sum"), lit(0)).as("neighbour_sum"),
          coalesce(col("sq2.square_count"), lit(0)).as("neighbour_count")
        )
        .groupBy(
          col("date"),
          col("latitude_square"),
          col("longitude_square")
        )
        .agg(
          max(col("square_sum")).as("square_sum"),
          max(col("square_count")).as("square_count"),
          max(col("square_avg")).as("square_avg"),
          sum(col("neighbour_sum")).as("neighbour_sum"),
          sum(col("neighbour_count")).as("neighbour_count")
        )
        .select(
          col("date"),
          col("latitude_square"),
          col("longitude_square"),
          col("square_sum"),
          col("square_count"),
          col("square_avg"),
          col("neighbour_sum"),
          col("neighbour_count"),
          (col("neighbour_sum") / col("neighbour_count")).cast(DecimalType(30, 15)).as("neighbour_avg")
        )

    salesPointsFiltered.as("sp")
      .join(squarePointsWithNeighbours.as("nb"),
        col("sp.date") === col("nb.date") &&
          col("sp.latitude_square") === col("nb.latitude_square") &&
          col("sp.longitude_square") === col("nb.longitude_square"), "inner"
      )
      .select(
        col("sp.terminal_id"),
        col("sp.date"),

        col("sp.trans_sum"),
        col("sp.trans_count"),
        col("sp.trans_avg"),

        col("sp.trans_sum_male"),
        col("sp.trans_sum_female"),

        col("sp.trans_count_male"),
        col("sp.trans_count_female"),

        col("sp.trans_avg_male"),
        col("sp.trans_avg_female"),

        col("sp.trans_sum_age_group_1"),
        col("sp.trans_sum_age_group_2"),
        col("sp.trans_sum_age_group_3"),
        col("sp.trans_sum_age_group_4"),
        col("sp.trans_sum_age_group_5"),
        col("sp.trans_sum_age_group_6"),
        col("sp.trans_sum_age_group_7"),
        col("sp.trans_sum_age_group_8"),
        col("sp.trans_sum_age_group_9"),

        col("sp.trans_count_age_group_1"),
        col("sp.trans_count_age_group_2"),
        col("sp.trans_count_age_group_3"),
        col("sp.trans_count_age_group_4"),
        col("sp.trans_count_age_group_5"),
        col("sp.trans_count_age_group_6"),
        col("sp.trans_count_age_group_7"),
        col("sp.trans_count_age_group_8"),
        col("sp.trans_count_age_group_9"),

        col("sp.trans_avg_age_group_1"),
        col("sp.trans_avg_age_group_2"),
        col("sp.trans_avg_age_group_3"),
        col("sp.trans_avg_age_group_4"),
        col("sp.trans_avg_age_group_5"),
        col("sp.trans_avg_age_group_6"),
        col("sp.trans_avg_age_group_7"),
        col("sp.trans_avg_age_group_8"),
        col("sp.trans_avg_age_group_9"),

        col("sp.trans_sum_morning"),
        col("sp.trans_sum_day"),
        col("sp.trans_sum_evening"),
        col("sp.trans_sum_night"),

        col("sp.trans_count_morning"),
        col("sp.trans_count_day"),
        col("sp.trans_count_evening"),
        col("sp.trans_count_night"),

        col("sp.trans_avg_morning"),
        col("sp.trans_avg_day"),
        col("sp.trans_avg_evening"),
        col("sp.trans_avg_night"),

        col("sp.return_sum"),
        col("sp.return_count"),
        col("sp.return_avg"),

        col("sp.country"),
        col("sp.day_of_week"),

        col("sp.latitude"),
        col("sp.longitude"),

        col("sp.latitude_square"),
        col("sp.longitude_square"),

        col("nb.square_sum"),
        col("nb.square_count"),
        col("nb.square_avg"),

        col("nb.neighbour_sum"),
        col("nb.neighbour_count"),
        col("nb.neighbour_avg"),

        col("sp.okrug_mt_num"),
        col("sp.region_id"),
        col("sp.internet_flag"),
        col("sp.category"),
        col("sp.holiday")
      )
      .repartition(col("date"))
  }
}
