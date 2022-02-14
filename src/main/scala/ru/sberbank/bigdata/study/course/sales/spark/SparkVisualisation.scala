package ru.sberbank.bigdata.study.course.sales.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import vegas.{Bar, Nom, Quant, Vegas}
import vegas.sparkExt._

object SparkVisualisation {
  def visualize(df: DataFrame, colX: String = "", colY: String = "", limit: Int = 100): Unit = {
    val dataFilter: DataFrame = df
      .withColumn("row_num", row_number.over(Window.orderBy(col(colY).desc_nulls_last)))
      .filter(col("row_num") <= limit)

    Vegas("BigData Spark App visualisation")
      .withDataFrame(dataFilter, limit)
      .encodeX(colX, Nom)
      .encodeY(colY, Quant)
      .mark(Bar)
      .show
  }
}
