package com.cloud.spark

import java.util.concurrent.atomic.AtomicBoolean

import com.cloud.spark.config.MonteCarloConfig
import com.cloud.spark.utils._
import com.google.common.util.concurrent.AtomicDouble
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object Simulator {

  val logger: Logger = LoggerFactory.getLogger(Simulator.getClass)

  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis() / 1000
    logger.debug("Starting simulation")
    //LogUtils.setLogLevel(Level.INFO)
    val configObj = MonteCarloConfig.configObj
    val spark = SparkSession.builder()
      .master("local")
      .appName("MonteCarloSimulator")
      .getOrCreate()

    // Call the simulate method which returns an array of portfolio value at the end of each day
    val portfolioTracker = simulate(spark, configObj)

    // Write the portfolio progress as a text file
    spark.sparkContext.parallelize(portfolioTracker).saveAsTextFile(configObj.config.getString("OUTPUT_PATH"))

    val end = System.currentTimeMillis() / 1000
    logger.debug("Ending simulation")
    logger.debug("Time taken to simulate : " + (1.0 * (end - start) / 60).toString)
  }

  def simulate(spark: SparkSession, configObj: MonteCarloConfig): ArrayBuffer[String] = {

    // Spark implicits import for spark conversions
    import spark.implicits._

    // Fetch stockSymbols, number of trials(sampling), days to simulate, and the initial portfolio amount
    val stockSymbols = configObj.config.getString("STOCKS_TO_BUY").split(",").toList
    val stockCount = stockSymbols.size
    val numTrials = configObj.config.getLong("NUMBER_OF_TRIALS")
    val days = configObj.config.getInt("DAYS_TO_SIMULATE")
    val portfolio = new AtomicDouble(configObj.config.getDouble("INITIAL_AMOUNT"))

    /*
    Call the @prepareData method to fetch and process the data and return the percent
    change of stock value for each trading day for the last 20 years for all the symbols
    as a DataFrame with 1st column as timestamp and the rest of the columns as percent
    change for each symbol
    */
    val pivotDf = prepareData(spark, stockSymbols)

    // Fetch the columns names (which represents all the stocks)
    val headers = pivotDf.columns

    // Creates a dictionary of symbol to the columns number of that symbol in the DataFrame
    val symbolColumnNumberMap = new mutable.HashMap[Int, String]()
    headers.foreach(columnName => {
      if (columnName != "timestamp") {
        symbolColumnNumberMap.put(headers.indexOf(columnName), columnName)
      }
    })

    // Convert the DataFrame to the rdd
    // Note : Im doing this step as I was unable to run .map on DataFrame effectively and fetch the randomized samples.
    val pivotRdd = pivotDf.rdd

    // Cache the RDD as it will be used several times (number of simulations)
    pivotRdd.cache

    // Fetch the count of the number of records fetched from the stock data archive
    val numHistoricalRecords = pivotRdd.count

    // Initialize the tracker and set capture the initial value
    val portfolioTracker = new ArrayBuffer[String]()
    portfolioTracker.append(String.format("Day %s, Portfolio value : %s", 0.toString, portfolio.get.toString))

    // Fetch the weights or the percentage to be invested in each stock
    val symbolWeights = configObj.config.getString("STOCK_BUY_PERCENT").split(",").map(x => x.toDouble)
    val symbolAndWeights: mutable.HashMap[String, Double] = mutable.HashMap() //("MSFT" -> 0.25d, "ADBE" -> 0.25d, "AMZN" -> 0.25d, "AAPL" -> 0.25d)
    if (symbolWeights.length == stockCount) {
      val totalWeight = symbolWeights.sum
      if (totalWeight == 100) {
        for (i <- 0 until stockCount) {
          symbolAndWeights.put(stockSymbols(i), symbolWeights(i) / 100d)
        }
      } else {
        // If the total percent is greater than 100 normalize it and assign weights
        for (i <- 0 until stockCount) {
          symbolAndWeights.put(stockSymbols(i), symbolWeights(i) / totalWeight * 100d)
        }
      }
    } else {
      // If faulty data, then assign equal weight to all stocks
      stockSymbols.foreach(x => symbolAndWeights.put(x, 1d / stockCount))
    }

    // Inside this loop we collect a random set of samples which is assumed to be normally distributed.
    // We then fetch different percentile of performance of the stocks and the portfolio.
    // We do this for 'days' no of times that is to say simulate 'n' days (which is input from config file
    for (day <- 1 to days) {
      logger.info("Simulation Day : " + day.toString)
      logger.debug("Portfolio Value : " + portfolio.get())

      // Initialize the stock percentile map to store the worst, likely and the best case scenarios of each stock and the portfolio
      val stockPercentileMap = new mutable.HashMap[String, Array[Double]]()

      // The fraction representing the fraction of data of the @pivotRdd which has to be fetched
      // as a randomized set of historical data which is approximately equal to the 'Number of Trials'
      // i.e representing 'Num of Trials' days of data representing our random sample of normally distributed historical data
      val fraction = 1.0 * numTrials / numHistoricalRecords

      // Using RDD.sample to fetch 'NUM_OF_TRIALS' records from the available historical data and making a DataFrame out of It.
      val resultOfTrials = pivotRdd.sample(withReplacement = true, fraction, System.currentTimeMillis).map(row => {
        val rowValues = row.toSeq
        val returnRow = new mutable.StringBuilder(rowValues.head.asInstanceOf[String])
        val total = new AtomicDouble()
        for (index <- 1 until rowValues.size) {
          val change = rowValues(index).asInstanceOf[Double]
          val weight = symbolAndWeights(symbolColumnNumberMap(index))
          returnRow.append(",").append(change)
          // The total portfolio change based on this days stock percentage change data
          total.addAndGet(weight * change)
        }
        returnRow.append(",").append(total)
        returnRow.toString
      }).toDF

      val numCols = headers.length + 1
      val changedHeaders: ArrayBuffer[String] = new ArrayBuffer[String]()
      headers.foreach(header => changedHeaders += header)
      changedHeaders += "portfolio"

      // The DataFrame contains all the percent change as a single column. We are splitting it to columns of each symbol and the portfolio percent change
      val finalRandomizedDf = resultOfTrials.withColumn("values", split(col("value"), ",")).select((1 until numCols).map(i => $"values".getItem(i).as(changedHeaders(i))): _*)

      changedHeaders.remove(0)
      changedHeaders.map(header => {
        val query = "cast(" + header + " as double) tmp"
        val totalPercentile = finalRandomizedDf.selectExpr(query).stat.approxQuantile("tmp", Array(0.05, 0.5, 0.95), 0)
        (header, totalPercentile.asInstanceOf[Array[Double]])
      }).foreach(x => stockPercentileMap.put(x._1, x._2))

      // Capture the portfolio value at the end of each day
      portfolio.set(portfolio.get() * (1 + stockPercentileMap("portfolio")(1) / 100))
      portfolioTracker.append(String.format("Day %s, Portfolio value : %s", day.toString, portfolio.get().toString))

      // Logic to buy and Sell at the end of the day based on the day's performance of the stocks held

      // Fetch the symbols which makes profit (likely percentile => median should be positive) and loss(likely percentile => median should be negative)
      val profitSymbols = stockPercentileMap.map(eachSymbol => (eachSymbol._1, eachSymbol._2(1))).filter(x => x._1 != "portfolio" || x._2 > 0).keys
      val lossSymbols = stockPercentileMap.map(eachSymbol => (eachSymbol._1, eachSymbol._2(1))).filter(x => x._1 != "portfolio" || x._2 <= 0).keys


      // If there are stock which are creating a loss to our portfolio,
      // then we sell that stock (Here assign its weight to 0 and ass that weight to a profit making stock)

      // The assumption here is all stock's prices are the similar, hence their weights carry equal price (neglecting percent change)

      // If our portfolio is making profit, then we compare the worst and best case scenario and if the worst case's standard deviation
      // is more than the best case's standard deviation then we sell the stock else we retain it as our portfolio is making profit and
      // we can endure the loss as the worst case is no that bad
      val stockWeightToAssign = new AtomicDouble(0)
      lossSymbols.foreach(x => {
        val percentiles = stockPercentileMap(x)
        val negativeDeviation = percentiles(1) - percentiles(0)
        val positiveDeviation = percentiles(2) - percentiles(1)
        if (stockPercentileMap("portfolio")(1) <= 0) {
          stockWeightToAssign.addAndGet(symbolAndWeights(x))
          symbolAndWeights.put(x, 0d)
        } else {
          if (negativeDeviation > positiveDeviation) {
            stockWeightToAssign.addAndGet(symbolAndWeights(x))
            symbolAndWeights.put(x, 0d)
          }
        }
      })

      // Buy the shares of profit making stocks, i.e assign weight to those stocks which are profit symbols
      val freeStockWeight = stockWeightToAssign.get
      if (freeStockWeight > 0 && profitSymbols.nonEmpty) {
        val eachSymbolGets = freeStockWeight / profitSymbols.size
        profitSymbols.foreach(x => symbolAndWeights.put(x, eachSymbolGets))
      }
      else {
        // No way to make profit right now. Assign equal weight to all symbols
        symbolAndWeights.foreach(x => symbolAndWeights.put(x._1, 1d / stockCount))
      }
    }
    logger.info("FINAL Portfolio Value : " + portfolio.get())
    portfolioTracker
  }

  /**
   * Utility Method to prepare the raw csv data into filtered dataframe with all the computed columns and pivoted on the stock symbol
   *
   * @param spark        The spark session object
   * @param stockSymbols List of stock symbols to invest in
   * @return DataSet[Row] with each row representing the daily data of a stock
   */
  def prepareData(spark: SparkSession, stockSymbols: List[String]): Dataset[Row] = {
    // import spark implicits for spark conversions
    import spark.implicits._

    // Schema object of the DataFrame to be created
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("open", DecimalType(10, 2), nullable = false),
      StructField("high", DecimalType(10, 2), nullable = true),
      StructField("low", DecimalType(10, 2), nullable = true),
      StructField("close", DecimalType(10, 2), nullable = true),
      StructField("volume", LongType, nullable = true),
      StructField("symbol", StringType, nullable = false)
    ))

    // The list of symbols whose stock is to be bought
    val symbolRdd = spark.sparkContext.parallelize(stockSymbols)

    // Fetching the stock data in csv format and create a Dataset from it
    val csvData = symbolRdd.flatMap(symbol => DataExtractor.getStockData(symbol)).toDS

    // Using the CSV Dataset and creating a DataFrame from the given schema
    val csvDataDf = spark.read.schema(schema).option("header", value = true).csv(csvData)

    // creating a Window partitioned by the symbol to calculate the percent change of a stock each day
    // The data is already sorted in descending order (as it is ingested that way from the API call).
    val windowSpec = Window.partitionBy("symbol").orderBy($"timestamp".desc)

    // Spark DataFrame UDF to change fraction to percent
    val percentColumn = udf((change: Double) => 100 * change)

    // Windowed operation on DataFrame to calculate percent change => 100 *  (Today's price - Yesterday's Price) / (Yesterday's Price) i.e 100 * (one row - next row) / (next Row) [As the columns are sorted by descending date]
    val withChangeDF = csvDataDf.withColumn("change", percentColumn(($"close" - when(lead("close", 1).over(windowSpec).isNull, 0).otherwise(lead("close", 1).over(windowSpec))) / when(lead("close", 1).over(windowSpec).isNull, 0).otherwise(lead("close", 1).over(windowSpec))))

    // Removing columns which are not required
    val trimmedDF = withChangeDF.drop("close", "high", "low", "volume", "open").filter($"change".isNotNull)
    trimmedDF.cache

    // At this stage we have a three columns => timestamp, change(percent change of stock), symbol (The stock's symbol)
    // Pivot the table on 'symbol' column making each unique symbol as a column and grouping by the 'timestamp' and aggregating(sum) on columns 'change' (which will anyway be individual values in this case with no actual summations)
    trimmedDF.groupBy($"timestamp").pivot("symbol").sum("change").filter(row => filterNull(row))
  }

  /**
   * Helper function to filter a dataframe row which has null in any of the columns
   *
   * @param row The sql.Row data structure
   * @return true if there are 0 null values in the row, false otherwise
   */
  def filterNull(row: Row): Boolean = {
    val bool = new AtomicBoolean(true)
    row.toSeq.foreach(each => {
      if (each == null) bool.set(false)
    })
    bool.get()
  }
}