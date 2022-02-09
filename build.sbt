ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "sber-bigdata-course",
    libraryDependencies ++= Seq(
      // command line arguments
      "org.rogach" %% "scallop" % "4.1.0",
      // configs
      "com.github.pureconfig" %% "pureconfig" % "0.14.0",
      // spark
      "org.apache.spark" %% "spark-core" % "2.4.8",
      "org.apache.spark" %% "spark-catalyst" % "2.4.8",
      "org.apache.spark" %% "spark-sql" % "2.4.8",
      "org.apache.spark" %% "spark-hive" % "2.4.8"
    ),
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-feature",
      "-unchecked",
      "-language:existentials",
      "-language:higherKinds",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    )
  )

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("javax", "servlet", _*)              => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
