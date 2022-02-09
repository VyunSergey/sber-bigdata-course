package ru.sberbank.bigdata.study.course.sales

import org.apache.spark.sql.SaveMode
import ru.sberbank.bigdata.study.course.sales.common.Arguments
import ru.sberbank.bigdata.study.course.sales.datamart.{SalesLocations, SalesPoints}
import ru.sberbank.bigdata.study.course.sales.spark.SparkConnection
import ru.sberbank.bigdata.study.course.sales.stage.{Calendar, Clients, Terminals, Transactions}
import ru.sberbank.bigdata.study.course.sales.stage.dict.{DictAgeGroup, DictGender, DictTransCategory}

import java.sql.Date

object Main {
  def main(args: Array[String]): Unit = {
    import SparkConnection.implicits._

    val arguments: Arguments = Arguments(args)
    val data: String = arguments.data()
    val start: Date = arguments.startDate()
    val end: Date = arguments.endDate()
    val mode: SaveMode = arguments.mode()

    data match {
      case "dict_age_group" =>
        DictAgeGroup.load(start, end, mode)
      case "dict_gender" =>
        DictGender.load(start, end, mode)
      case "dict_trans_category" =>
        DictTransCategory.load(start, end, mode)
      case "calendar" =>
        Calendar.load(start, end, mode)
      case "clients" =>
        Clients.load(start, end, mode)
      case "terminals" =>
        Terminals.load(start, end, mode)
      case "transactions" =>
        Transactions.load(start, end, mode)
      case "sales_points" =>
        SalesPoints.load(start, end, mode)
      case "sales_locations" =>
        SalesLocations.load(start, end, mode)
    }
  }
}
