package ru.sberbank.bigdata.study.course.sales.common

import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto.deriveReader

import java.nio.file.Path

object Configuration {
  case class Config(rootPaths: Map[String, Path])

  lazy val conf: Config = ConfigSource.default.loadOrThrow[Config]

  implicit val appConfReader: ConfigReader[Config] = deriveReader[Config]
}
