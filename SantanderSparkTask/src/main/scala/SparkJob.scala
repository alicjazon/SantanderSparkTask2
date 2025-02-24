import PathHelper.{getInputCsvFilePath, getOsPath}
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

abstract class SparkJob(spark: SparkSession, ingestionPath: PathHelper, scheduledDate: String) {

  def run(): Unit = {
    val inputFilePath = getInputCsvFilePath(ingestionPath, scheduledDate)
    read(inputFilePath) match {
      case Some(inputDf) => write(transform(inputDf))
      case None => println(s"Warning! Cannot read file in path $inputFilePath. Registry will not be updated.")
    }
  }

  def read(inputFilePath: String): Option[DataFrame] = {
    val schemaPath = getOsPath(ingestionPath.schemaPath)
    Try(
      spark.read
        .option("delimiter", ";")
        .schema(os.read(schemaPath))
        .csv(inputFilePath)
    ).toOption
  }

  def transform(inputDf: DataFrame): DataFrame

  def write(inputDf: DataFrame, mode: SaveMode = Append): Unit = inputDf
    .write.mode(mode)
    .partitionBy("PARTITION_DATE")
    .parquet(ingestionPath.outputPath)
}
