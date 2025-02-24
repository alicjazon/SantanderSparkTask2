import PathHelper.{exchangeRatesPath, receiversPath}
import org.apache.spark.sql.SparkSession

object SparkJobLauncher extends App {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("app")
    .getOrCreate()
  val scheduledDate = "25.01.2024"
//  new CsvIngestion(spark, scheduledDate).run()
//  new UpdateLatestRegistry(spark, receiversPath, scheduledDate).run()
//  new UpdateLatestRegistry(spark, exchangeRatesPath, scheduledDate).run()
  new GuaranteesReport(spark, scheduledDate).run()
}
