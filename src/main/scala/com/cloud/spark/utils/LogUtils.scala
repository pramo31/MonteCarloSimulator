package com.cloud.spark.utils

import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.LoggerFactory

object LogUtils {
  def setLogLevel(logLevel: Level): Unit = {
    val loggers = Seq(
      "com.cloud.spark", "org.apache.http", "org.spark_project", "org.apache.spark", "io.netty", "org.apache.hadoop"
    )
    loggers.foreach {
      name =>
        val logger = LoggerFactory.getLogger(name).asInstanceOf[Logger]
        logger.setLevel(logLevel)
        logger.setAdditive(false)
    }
  }
}
