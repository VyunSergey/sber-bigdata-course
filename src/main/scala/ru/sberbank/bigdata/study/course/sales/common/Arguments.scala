package ru.sberbank.bigdata.study.course.sales.common

import org.apache.spark.sql.SaveMode
import org.rogach.scallop.exceptions.{Help, Version}
import org.rogach.scallop.{Compat, ScallopConf, ScallopOption}

import java.sql.Date
import scala.util.Try

case class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("1.0.0 (c) 2022 Vyun Sergey")

  private val supportedData: List[String] = List(
    "dict_age_group",
    "dict_gender",
    "dict_trans_category",
    "calendar",
    "clients",
    "terminals",
    "transactions",
    "sales_points",
    "sales_locations"
  )

  banner(s"""\nДобро пожаловать на курс BigData со Spark!
           |Надеюсь, Вам понравится, удачи!
           |\nЗапуск: --data=<Название датасета> [--c] [--s] [--start-date=<Дата начала>] [--end-date=<Дата конца>] [--mode=<Режим расчета>]
           |Пример: --data=transactions --c --s --start-date=2021-06-01 --end-date=2021-06-05 --mode=Append
           |  параметр --data необходимо указывать обязательно
           |  параметр --count можно не указывать, тогда возьмется значение 'false'
           |  параметр --show можно не указывать, тогда возьмется значение 'false'
           |  параметр --start-date можно не указывать, тогда возьмется значение '1900-01-01'
           |  параметр --end-date можно не указывать, тогда возьмется значение '5999-12-31'
           |  параметр --mode можно не указывать, тогда возьмется значение 'Overwrite'
           |\nОписание параметров:
           |""".stripMargin)
  footer("\nЕсли возникли проблемы или вопросы, обращайтесь по каналам связи")

  override def onError(e: Throwable): Unit = e match {
    case _: Help =>
      builder.printHelp()
      Compat.exit(0)
    case Version =>
      builder.vers.foreach(println)
      Compat.exit(0)
    case _ =>
      builder.printHelp()
      super.onError(e)
  }

  val data: ScallopOption[String] = opt[String](
    name = "data",
    descr = s"Название датасета, поддерживаются: ${supportedData.mkString(", ")}",
    required = true,
    validate = supportedData.contains
  )

  val countFlg: ScallopOption[Boolean] = toggle(
    name = "count",
    short = 'c',
    descrYes = "Флаг подсчета количества строк в датасете",
    required = false,
    default = Some(false)
  )

  val showFlg: ScallopOption[Boolean] = toggle(
    name = "show",
    short = 's',
    descrYes = "Флаг вывода в консоль примера данных в датасете",
    required = false,
    default = Some(false)
  )

  val startDate: ScallopOption[Date] = opt[String](
    name = "start-date",
    descr = s"Бизнес дата начала периода расчета, по умолчанию: $None",
    required = false,
    default = Some("1900-01-01"),
    validate = (str: String) => Try(Date.valueOf(str)).isSuccess
  ).map(Date.valueOf)

  val endDate: ScallopOption[Date] = opt[String](
    name = "end-date",
    descr = s"Бизнес дата конца периода расчета, по умолчанию: $None",
    required = false,
    default = Some("5999-12-31"),
    validate = (str: String) => Try(Date.valueOf(str)).isSuccess
  ).map(Date.valueOf)

  val mode: ScallopOption[SaveMode] = opt[String](
    name = "mode",
    descr = s"Режим расчета, по умолчанию: ${SaveMode.Overwrite.name}," +
      s" поддерживаются: ${SaveMode.values.map(_.name).mkString(", ")}",
    required = false,
    default = Some(SaveMode.Overwrite.name),
    validate = SaveMode.values.map(_.name).contains
  ).map(SaveMode.valueOf)

  verify()
}
