package com.cloud.spark.config

import com.cloud.spark.utils.ConfigReader
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

class MonteCarloConfig(val configFileName: String = Constants.configFile.toString()) {

  final val logger: Logger = LoggerFactory.getLogger(MonteCarloConfig.getClass)

  val config: Config = ConfigReader.readConfig(configFileName)

}

object MonteCarloConfig {
  val configObj = new MonteCarloConfig()
}
