package com.cloud.spark.utils

import java.util.concurrent.atomic.AtomicInteger

import com.cloud.spark.config.MonteCarloConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.slf4j.{Logger, LoggerFactory}

import scala.io

object DataExtractor {

  val logger: Logger = LoggerFactory.getLogger(DataExtractor.getClass)

  /**
   * Returns the text content from a REST API Endpoint. Returns a blank String if there is a problem.
   *
   * @param url    The url (API Endpoint) to fetch the stock data
   * @param symbol The stock symbol whose data is to be fetched
   * @return String of retrieved stock contents in a pseudo CSV format
   */
  def getRestContent(url: String, symbol: String): String = {
    logger.debug(String.format("URL : %s", url))
    val httpClient = HttpClients.createDefault
    val httpResponse = httpClient.execute(new HttpGet(url))
    val statusCode = httpResponse.getStatusLine.getStatusCode
    val csvContent: StringBuilder = new StringBuilder("")
    val count = new AtomicInteger()
    if (statusCode == 200) {
      val entity = httpResponse.getEntity
      if (entity != null) {
        val inputStream = entity.getContent

        val lines = io.Source.fromInputStream(inputStream).getLines
        lines.foreach(line => {
          val row = line.concat(",").concat(symbol)
          if (!row.contains("timestamp")) {
            count.addAndGet(1)
            csvContent.append(row).append("\n")
          }
        })
        inputStream.close()
      }
    }
    httpClient.close()
    logger.info(String.format("Rows returned from %s endpoint call : %s", symbol, count.get().toString))
    csvContent.toString
  }

  /**
   * Helper method used to fetch stock data by making an API call
   *
   * @param symbol The stock symbol whose data is to be fetched
   * @return Array of the rows representing each days stock data
   */
  def getStockData(symbol: String): Array[String] = {
    val configObj = MonteCarloConfig.configObj

    val apiKey = configObj.config.getString("API_KEY")
    val outputSize = configObj.config.getString("OUTPUT_SIZE")

    val urlBuilder = configObj.config.getString("API_ENDPOINT")
      .replace("{output_size}", outputSize)
      .replace("{api_key}", apiKey)
      .replace("{symbol}", symbol)

    val header = "timestamp,open,high,low,close,volume,symbol"
    val stockData = new StringBuilder(header).append("\n")

    stockData.append(getRestContent(urlBuilder, symbol))
    stockData.toString.split("\n")
  }
}
