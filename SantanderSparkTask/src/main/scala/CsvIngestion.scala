import PathHelper.transactionsPath
import org.apache.spark.sql.functions.{col, from_json, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CsvIngestion(spark: SparkSession, scheduledDate: String, ingestionPath: PathHelper = transactionsPath)
  extends SparkJob(spark, ingestionPath, scheduledDate) {

  override def transform(inputDf: DataFrame): DataFrame = {
    inputDf
      .withColumn("row_number", monotonically_increasing_id())
      .withColumn("GUARANTORS", from_json(col("GUARANTORS"), guarantorsSchema, Map("mode" -> "FAILFAST")))
  }

  override def write(inputDf: DataFrame, mode: SaveMode): Unit = super.write(inputDf, SaveMode.Overwrite)

  private val guarantorsSchema = ArrayType(
    StructType(Seq(
      StructField("NAME", StringType),
      StructField("PERCENTAGE", DecimalType(3, 3))
    ))
  )
}
