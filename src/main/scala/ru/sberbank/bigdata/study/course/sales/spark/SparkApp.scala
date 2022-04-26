package ru.sberbank.bigdata.study.course.sales.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}
import ru.sberbank.bigdata.study.course.sales.common.Configuration

import java.nio.file.Path
import java.sql.Date
import scala.util.{Failure, Success, Try}

trait SparkApp extends Logging {

  /*
   * Название датасета
   * */
  val name: String

  /*
   * Опционально название поля партицирования данных
   * */
  val partitionColName: Option[String] = None

  /*
   * Путь хранения данных в файловой системе
   * */
  def path: Path = {
    val rp = rootPath(name)
    if (rp.endsWith(name)) rp else rp.resolve(name)
  }

  /*
   * Метод подсчета количества данных
   * */
  def count(start: Date = minStart, end: Date = maxEnd)(implicit spark: SparkSession): Long = {
    val dataFrame: DataFrame = Try {
      get()
    } getOrElse {
      gen(start, end)
    }

    val count: Long = partitionColName.map { name =>
      dataFrame.filter(col(name).between(start, end)).count
    }.getOrElse(dataFrame.count)

    println(prettyInfo(s"Successfully counted rows in data $name count=$count"))
    count
  }

  /*
   * Метод вывода в консоль примера данных
   * Можно считать количество строк или сумму по полю `sumColName` в разрезе поля `groupColName`
   * По умолчанию выводится 20 строк
   * Если задать поле `sumColName` то по нему будет считаться сумма
   * Если задать поле `groupColName` то в разрезе него будет происходить расчет
   * */
  def show(start: Date = minStart,
           end: Date = maxEnd,
           groupColName: Option[String] = None,
           sumColName: Option[String] = None,
           lines: Int = 20,
           truncate: Boolean = false)(implicit spark: SparkSession): Unit = {
    val dataFrame: DataFrame = Try {
      get()
    } getOrElse {
      gen(start, end)
    }

    val dataFrameFiltered: DataFrame =
      partitionColName.map { name =>
        dataFrame.filter(col(name).between(start, end))
      }.getOrElse(dataFrame)

    (groupColName, sumColName) match {
      case (Some(groupCol), Some(sumCol)) =>
        dataFrameFiltered
          .groupBy(col(groupCol))
          .agg(sum(col(sumCol)).cast(DecimalType(38, 10)).as(s"sum of $sumCol"))
          .orderBy(col(s"sum of $sumCol").desc)
          .show(lines, truncate)
      case (Some(groupCol), None) =>
        dataFrameFiltered
          .groupBy(col(groupCol))
          .count
          .orderBy(col("count").desc)
          .show(lines, truncate)
      case (None, Some(sumCol)) =>
        dataFrameFiltered
          .select(sum(col(sumCol)).cast(DecimalType(38, 10)).as(s"sum of $sumCol"))
          .show(lines, truncate)
      case (None, None) =>
        dataFrameFiltered.show(lines, truncate)
    }
  }

  /*
   * Метод визуализации данных с помощью графиков
   * Можно считать количество строк или сумму по полю `sumColName` в разрезе поля `groupColName`
   * По умолчанию считается количество без разреза
   * Если задать поле `sumColName` то по нему будет считаться сумма
   * Если задать поле `groupColName` то в разрезе него будет происходить расчет
   * */
  def visualize(start: Date = minStart,
                end: Date = maxEnd,
                groupColName: Option[String] = None,
                sumColName: Option[String] = None,
                limit: Int = 100)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val dataFrame: DataFrame = Try {
      get()
    } getOrElse {
      gen(start, end)
    }

    val dataFrameFiltered: DataFrame =
      partitionColName.map { name =>
        dataFrame.filter(col(name).between(start, end))
      }.getOrElse(dataFrame)

    (groupColName.orElse(partitionColName), sumColName) match {
      case (Some(groupCol), Some(sumCol)) =>
        SparkVisualisation.visualize(
          colX = groupCol,
          colY = s"sum of $sumCol",
          limit = limit,
          df = dataFrameFiltered
            .groupBy(col(groupCol))
            .agg(sum(col(sumCol)).cast(DecimalType(38, 10)).as(s"sum of $sumCol"))
            .orderBy(col(s"sum of $sumCol").desc)
        )
      case (Some(groupCol), None) =>
        SparkVisualisation.visualize(
          colX = groupCol,
          colY = "count",
          limit = limit,
          df = dataFrameFiltered
            .groupBy(col(groupCol))
            .count
            .orderBy(col("count").desc)
        )
      case (None, Some(sumCol)) =>
        SparkVisualisation.visualize(
          colX = "data",
          colY = s"sum of $sumCol",
          limit = limit,
          df = Seq(("data", dataFrameFiltered
            .select(sum(col(sumCol)).cast(DecimalType(38, 10)).as(s"sum of $sumCol")))
          ).toDF("data", s"sum of $sumCol")
        )
      case (None, None) =>
        SparkVisualisation.visualize(
          colX = "data",
          colY = "count",
          limit = limit,
          df = Seq(("data", dataFrameFiltered.count)).toDF("data", "count")
        )
    }
  }

  /*
   * Метод расчета данных
   * Содержит всю логику преобразования и обработки данных
   * */
  def gen(start: Date = minStart, end: Date = maxEnd)(implicit spark: SparkSession): DataFrame

  /*
   * Метод получения данных по имени `name` или пути `path`
   * */
  def get(name: String = name, path: Path = path)(implicit spark: SparkSession): DataFrame = {
    logInfo(s"Getting data $name from ${path.toAbsolutePath.toString}")
    Try {
      spark.table(name)
    } orElse Try {
      spark.read
        .format("csv")
        .options(
          Map(
            "header" -> "true",
            "sep" -> ";",
            "encoding" -> "utf-8"
          )
        )
        .load(s"file:///${path.toAbsolutePath.toString}")
    } match {
      case Success(dataFrame) =>
        logInfo(s"Successfully got data $name from ${path.toAbsolutePath.toString}")
        dataFrame
      case Failure(exp) =>
        logError(s"Error while getting data $name from ${path.toAbsolutePath.toString}", exp)
        throw exp
    }
  }

  /*
   * Метод сохранения рассчитанных данных по пути `path` в файловой системе
   * Поддерживается несколько режимов сохранения:
   * SaveMode.Append - добавить данные к уже имеющимся
   * SaveMode.Overwrite - перезаписать данные
   * SaveMode.ErrorIfExists - прекратить работу с ошибкой, если уже что-то имеется
   * SaveMode.Ignore - не записывать новые данные, если уже что-то имеется
   * По умолчанию используется режим SaveMode.Overwrite т.е. данные перезаписываются
   * */
  def load(start: Date,
           end: Date,
           mode: SaveMode = SaveMode.Overwrite,
           partColNames: Seq[String] = partitionColName.toSeq)(implicit spark: SparkSession): Unit = {
    logInfo(s"Loading data $name to ${path.toAbsolutePath.toString}")
    Try {
      val writer: DataFrameWriter[Row] = gen(start, end).write
        .format("csv")
        .mode(mode)
        .options(
          Map(
            "header" -> "true",
            "sep" -> ";",
            "encoding" -> "utf-8",
            "path" -> s"file:///${path.toAbsolutePath.toString}"
          )
        )
      (if (partColNames.nonEmpty) writer.partitionBy(partColNames: _*) else writer)
        .saveAsTable(name)
    } match {
      case Success(_) =>
        logInfo(s"Successfully loaded data $name to ${path.toAbsolutePath.toString}")
      case Failure(exp) =>
        logError(s"Error while loading data $name to ${path.toAbsolutePath.toString}", exp)
        throw exp
    }
  }

  private val minStart: Date = Date.valueOf("1900-01-01")
  private val maxEnd: Date = Date.valueOf("5999-12-31")

  private def rootPath(name: String): Path = Try {
    Configuration.conf.rootPaths(name)
  } match {
    case Success(p) => p
    case Failure(exp) =>
      logError(s"Error while getting root paths for $name from config file application.conf", exp)
      throw exp
  }

  override def logInfo(msg: => String): Unit = super.logInfo(prettyInfo(msg))
  override def logInfo(msg: => String, throwable: Throwable): Unit = super.logInfo(prettyInfo(msg), throwable)
  override def logWarning(msg: => String): Unit = super.logWarning(prettyInfo(msg))
  override def logWarning(msg: => String, throwable: Throwable): Unit = super.logWarning(prettyInfo(msg), throwable)
  override def logError(msg: => String): Unit = super.logError(prettyInfo(msg))
  override def logError(msg: => String, throwable: Throwable): Unit = super.logError(prettyInfo(msg), throwable)
  override def logDebug(msg: => String): Unit = super.logDebug(prettyInfo(msg))
  override def logDebug(msg: => String, throwable: Throwable): Unit = super.logDebug(prettyInfo(msg), throwable)
  override def logTrace(msg: => String): Unit = super.logTrace(prettyInfo(msg))
  override def logTrace(msg: => String, throwable: Throwable): Unit = super.logTrace(prettyInfo(msg), throwable)

  private def prettyInfo(message: String, hs: Char = '-', vs: Char = '|'): String = {
    val rowsMaxLength: Int = Try(message.split("\n").map(_.length).max).getOrElse(0)
    val horizontalSep: String = vs.toString + " " + hs.toString * rowsMaxLength + " " + vs.toString
    ("" +: horizontalSep +: message.split("\n").map { line =>
      vs.toString + " " + line + " " * (rowsMaxLength - line.length) + " " + vs.toString
    } :+ horizontalSep).mkString("\n")
  }
}
