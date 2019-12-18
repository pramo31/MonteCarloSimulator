//ThisBuild / organization := "com.cloud.spark"
ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version      := "0.1.0"
ThisBuild / name      := "SparkModel"

lazy val root = (project in file("."))
  .settings(
	libraryDependencies ++= Seq("ch.qos.logback" % "logback-examples" % "1.3.0-alpha4","com.typesafe" % "config" % "1.2.1", "commons-lang" % "commons-lang" % "2.6", "com.ctc.wstx" % "woodstox-osgi" % "3.2.1.1", "org.scalatest" %% "scalatest" % "3.0.8" % "test", "org.scalaj" % "scalaj-http_2.12" % "2.4.2","org.apache.httpcomponents" % "httpclient" % "4.5.6", "org.apache.spark" %% "spark-sql" % "2.3.1", "org.apache.spark" %% "spark-core" % "2.3.1", "org.apache.spark" %% "spark-sql" % "2.3.1", "org.apache.spark" %% "spark-core" % "2.3.1", "com.google.code.gson" % "gson" % "2.8.5")
  )
  
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}