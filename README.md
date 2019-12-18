**Running the project on AWS EMR**

*Prerequisites*
- Latest version of SBT should be installed in your system/cluster depending on where you will compile the project
- Create an aws emr instance with default configs, i.e a Spark Cluster with others as optional
- Upload the runnable jar to a aws s3 bucket (steps to package jar below).

**Note on Config File**
- *Before building the jar you can change the config file in ./src/main/resources as required.*
- *You can configure the spark you want to run in MonteCarlo.conf file in src/main/resources of the project.*
- The stock and it's initial weights, initial portfolio amount, number of days to simulate and the number of trials are all configurable.


*Steps to run Spark Job*
1) Go to the root folder Homework3
2) Enter the following command : 'sbt clean compile test' to run the unit tests
3) Run the command 'sbt assembly'. This will create a packaged jar file at this location -> ./target/scala-2.13/root-assembly-0.1.0.jar
4) Upload this jar to the created aws s3 bucket
5) Create a step in the AWS EMR Step. There are two ways to do this:
	a) Method 1 - Custom Jar
		- Click on Steps tab in the EMR resource in AWS Console
		- Select Custom Jar as Step Type.
		- Set The Jar Location to the S3 path where you have uploaded the JAR to.
		- Set the arguments as the spark submit command. See below for the spark submit commands to run the job locally or on YARN
		- Click on Add and wait for the completion of the job.
    b) Method 2 - Spark Application
    	- Click on Steps tab in the EMR resource in AWS Console
		- Select Spark Application as Step Type.
		- Enter any Name and select the Deploy Mode as 'cluster'
		- In spark submit options enter the following
		    => --class "com.cloud.spark.Simulator" --master yarn
        - Click on Add and wait for the completion of the job.
6) Output will be present in the HDFS path set in the config file ("OUTPUT_PATH")

*Note: Spark Submit commands
1) To run it locally => spark-submit --class "com.cloud.spark.Simulator" --master local "{jar-path}"
2) To run on cluster using yarn => spark-submit --class "com.cloud.spark.Simulator" --master yarn --deploy-mode cluster "{jar-path}"*

*Note: An alternative is to copy the jar to the cluster via winscp and run it locally using spark-submit commands, or put the binaries into the cluster and directly run the 'sbt run' command in a spark cluster driver.
The Spark Command => spark-submit --class "com.cloud.spark.Simulator" --master local "{jar-path}"*


**Important Info**

**- YOUTUBE LINK : **


Working of the Simulation

1) Data Preparation
- Initially all the historical data of the stocks interests to buy are fetched using 'alphavantage' API. The complete historical data available is fetched.
- The percent change is calculated from by calculating the difference of consecutive days opening value of a stock by the previous days stock as a percentage.
- Then all the columns except the symbol, timestamp and percent change columns are dropped from the DataFrame.
- We then pivot the table on symbols on grouping with the timestamp resulting with all the symbols becoming columns and each unique timestamp becomes a row. The is the final DataFrame which will be reused several times. We go ahead and cache an RDD version of this table (The Dataframe version was not able to use .map function as required)

2) The simulation/iteration
- We iterate through the DataFrame creating randomized samples calculating likely stock changes and portfolio profit/loss for 'n' times where 'n' is an input config representing number of days to simulate the portfolio.
- The randomization is done using sampling by fetching a specific number of rows (repeated) as decided by the input. The rows ae all taken from historical data. Hence this will create a random normal distribution of the stock variation. Hence taking a median of this  will be the most likely outcome of that day of our simulation.
- We do this percentile calculation for all our stocks and the overall portfolio.
- If at the end of our day, if our portfolio is at loss, then we sell all our loss making symbols (as we have the percentile of individual symbols also). If there are no profit making symbols, then we leave the portfolio with equal share from all stocks.
- if our portfolio is making profit, then we check for loss making stocks. If these stocks


-- Performance:

- Local System (Windows)
-> Simulation for 1 day - Almost 1 minute
--> Simulation for 30 days - 24 minutes

- AWS EMR
--> Simulation for 30 days - 17 minutes

Note: The parallelization of the implementation is not very effective, hence EMR performance is not great
