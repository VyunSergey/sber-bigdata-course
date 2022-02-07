ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "sber-bigdata-course",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.8",
      "org.apache.spark" %% "spark-catalyst" % "2.4.8",
      "org.apache.spark" %% "spark-sql" % "2.4.8",
      "org.apache.spark" %% "spark-hive" % "2.4.8"
    )
  )
