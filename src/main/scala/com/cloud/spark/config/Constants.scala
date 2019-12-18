package com.cloud.spark.config

import com.cloud.spark.config

object Constants extends Enumeration {

  type Constants = Value

  val configFile: config.Constants.Value = Value("MonteCarlo")

}
