package ru.sberbank.bigdata.study.course.sales.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}
import ru.sberbank.bigdata.study.course.sales.common.Configuration

import java.nio.file.Path
import java.sql.Date
import scala.util.{Failure, Success, Try}

trait SparkApp extends Logging {
  /*
   *
   * */
  val name: String

  /*
   *
   * */
  val partitionColumnNames: Seq[String] = Nil

  /*
   *
   * */
  def path: Path = {
    val rp = rootPath(name)
    if (rp.endsWith(name)) rp else rp.resolve(name)
  }

  /*
   *
   * */
  def count(start: Date = minStart, end: Date = maxEnd)(implicit spark: SparkSession): Long = {
    (Try {
      get()
    } getOrElse {
      gen(start, end)
    }).count()
  }

  /*
   *
   * */
  def show(start: Date = minStart, end: Date = maxEnd, lines: Int = 20, truncate: Boolean = true)(implicit spark: SparkSession): Unit = {
    (Try {
      get()
    } getOrElse {
      gen(start, end)
    }).show(lines, truncate)
  }

  /*
   *
   * */
  def gen(start: Date = minStart, end: Date = maxEnd)(implicit spark: SparkSession): DataFrame

  /*
   *
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
   *
   * */
  def load(start: Date,
           end: Date,
           mode: SaveMode = SaveMode.Overwrite,
           partColNames: Seq[String] = partitionColumnNames)(implicit spark: SparkSession): Unit = {
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
