package ru.sberbank.bigdata.study.course.sales

import org.apache.spark.sql.SaveMode
import ru.sberbank.bigdata.study.course.sales.common.Arguments
import ru.sberbank.bigdata.study.course.sales.datamart.{SalesLocations, SalesPoints}
import ru.sberbank.bigdata.study.course.sales.spark.{SparkApp, SparkConnection}
import ru.sberbank.bigdata.study.course.sales.stage.{Calendar, Clients, Terminals, Transactions}
import ru.sberbank.bigdata.study.course.sales.stage.dict.{DictAgeGroup, DictGender, DictTransCategory}

import java.sql.Date

object Main {
  def main(args: Array[String]): Unit = {
    import SparkConnection.implicits._

    val arguments: Arguments = Arguments(args)
    val data: String = arguments.data()
    val countFlg: Boolean = arguments.countFlg()
    val showFlg: Boolean = arguments.showFlg()
    val start: Date = arguments.startDate()
    val end: Date = arguments.endDate()
    val mode: SaveMode = arguments.mode()
    val sparkApp: SparkApp = appMatcher(data)

    if (countFlg) sparkApp.count(start, end)
    if (showFlg) sparkApp.show(start, end)
    if (!countFlg && !showFlg) sparkApp.load(start, end, mode)
  }

  val appMatcher: String => SparkApp = {
    case "dict_age_group" => DictAgeGroup
    case "dict_gender" => DictGender
    case "dict_trans_category" => DictTransCategory
    case "calendar" => Calendar
    case "clients" => Clients
    case "terminals" => Terminals
    case "transactions" => Transactions
    case "sales_points" => SalesPoints
    case "sales_locations" => SalesLocations
  }
}
