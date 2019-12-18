package com.cloud.spark

import com.cloud.spark.Simulator.simulate
import com.cloud.spark.config.MonteCarloConfig
import com.cloud.spark.utils._
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.SparkSession
import org.scalatest._

class UnitTests extends FunSuite {

  test("The config reader reads the value from config file") {
    val config: Config = ConfigReader.readConfig("test")
    val actual = config.getString("TestConfig")
    val expected = "TestValue"
    assert(actual == expected)
  }
  test("Invalid configName throws exceptions") {
    val config: Config = ConfigReader.readConfig("test")
    assertThrows[ConfigException.Missing] {
      config.getString("InvalidConfigName")
    }
  }
  test("Utility function to fetch data from API endpoint Positive") {
    val url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&apikey=demo&datatype=csv"
    val symbol = "MSFT"
    val stockData = DataExtractor.getRestContent(url, symbol)
    val actual = stockData.toString.split("\n").toList.size
    val expected = 100
    assert(actual == expected)
  }
  test("Utility function to fetch data from API endpoint Negative") {
    val url = "https://www.alphavantage.co/query?functio=TIME_SERIES_DAILY&symbol=MSFT&apikey=464654"
    val symbol = "MSFT"
    val actual = DataExtractor.getRestContent(url, symbol).size
    assert(actual < 100)
  }
  test("Test Helper method to fetch stock data") {
    val symbol = "MSFT"
    val actual = DataExtractor.getStockData(symbol)
    assert(actual != null)
    assert(actual.length == 101)
  }
  test("Simulator Integration Test") {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MonteCarloSimulator")
      .getOrCreate()

    val configObj = MonteCarloConfig.configObj

    val portfolioTracker = simulate(spark, configObj)
    val actual = portfolioTracker.size
    val expected = 2
    assert(actual == expected)

  }
}