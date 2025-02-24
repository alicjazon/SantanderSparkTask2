import PathHelper.{exchangeRatesPath, receiversPath, transactionsPath}
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class GuaranteesReport(spark: SparkSession, scheduledDate: String) {

  def run(): Unit = {
    val inputsMap = read()
    val transformedInputs = transform(inputsMap)
    write(transformedInputs)
  }

  def read(): Map[String, DataFrame] = {
    Map(
      "transactions" -> spark.read.parquet(transactionsPath.outputPath).filter(col("PARTITION_DATE") === scheduledDate),
      "receiversRegistry" -> spark.read.parquet(receiversPath.outputPath),
      "exchangeRatesRegistry" -> spark.read.parquet(exchangeRatesPath.outputPath).select("FROM_CURRENCY", "RATE")
    )
  }

  def transform(inputsMap: Map[String, DataFrame]): DataFrame = {
    val joinedDf = applyFiltersAndRemoveDuplicates(inputsMap("transactions"))
      .join(inputsMap("receiversRegistry"), inputsMap("receiversRegistry").col("RECEIVER_NAME") === inputsMap("transactions").col("RECEIVER_NAME"), "left_semi")
      .join(inputsMap("exchangeRatesRegistry"), col("CURRENCY") === col("FROM_CURRENCY"))
      .withColumn("GUARANTORS", explode(col("GUARANTORS")))
      .withColumn("RATE", regexp_replace(col("RATE"), ",", ".").cast(DecimalType(3, 3)))
      .select(col("GUARANTORS.NAME"), col("GUARANTORS.PERCENTAGE"), col("TYPE"), col("COUNTRY"), col("RATE"), col("AMOUNT"), col("PARTITION_DATE"))

    val finaldf = calculateGuaranteesAmountPerType(joinedDf)
    finaldf.show
    finaldf
  }

  def write(inputDf: DataFrame, mode: SaveMode = Append): Unit = inputDf
    .write.mode(mode)
    .partitionBy("PARTITION_DATE")
    .csv("data/reports")

  private def applyFiltersAndRemoveDuplicates(df: DataFrame) = {
    val window = Window.partitionBy("TRANSACTION_ID").orderBy("row_number")
    val firstDayOfMonth = date_format(to_date(col("PARTITION_DATE"), "dd.MM.yyyy"), "MM.yyyy")

    df
      .filter(col("STATUS").isin("OPEN", "PENDING"))
      .filter(date_format(to_date(col("DATE"), "d.MM.yyyy"), "MM.yyyy") >= firstDayOfMonth)
      .withColumn("row", row_number().over(window))
      .filter(col("row") === 1).drop("row", "row_number")
  }

  private def calculateGuaranteesAmountPerType(df: DataFrame) = df
    .groupBy("NAME", "COUNTRY", "RATE", "PARTITION_DATE")
    .pivot("TYPE", Seq("CLASS_A", "CLASS_B", "CLASS_C", "CLASS_D"))
    .agg(first(col("PERCENTAGE") * col("AMOUNT")))
    .na.fill(0)
    .withColumn("AVG_CLASS_A", calculateAverageAmountPerCountry("CLASS_A"))
    .withColumn("AVG_CLASS_B", calculateAverageAmountPerCountry("CLASS_B"))
    .withColumn("AVG_CLASS_C", calculateAverageAmountPerCountry("CLASS_C"))
    .withColumn("AVG_CLASS_D", calculateAverageAmountPerCountry("CLASS_D"))
   // .drop("RATE")

  private def calculateAverageAmountPerCountry(typeName: String) =
    avg(typeName).over(Window.partitionBy("COUNTRY")) * col("RATE")
}
