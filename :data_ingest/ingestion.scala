import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

// Start a Spark session
val spark = SparkSession.builder.appName("DataCleaningInteractive").getOrCreate()

// Function to read a CSV file with additional options
def readCsvFile(filePath: String): DataFrame = {
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .option("dateFormat", "yyyy-MM-dd")
    .csv(filePath)
}

// Read the primary dataset from hdfs file called "Bus_Breakdown_and_Delays_20231105.csv"
val df = readCsvFile("Bus_Breakdown_and_Delays_20231105.csv")

