import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("app")
    .getOrCreate()
}
