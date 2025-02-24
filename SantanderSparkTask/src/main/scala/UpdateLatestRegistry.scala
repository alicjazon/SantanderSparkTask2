import PathHelper.getOsPath
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class UpdateLatestRegistry(spark: SparkSession, ingestionPath: PathHelper, scheduledDate: String)
  extends SparkJob(spark, ingestionPath, scheduledDate) {

  override def transform(inputDf: DataFrame): DataFrame = {
    val lastPartitionDate = getLastPartitionDate
    inputDf
      .filter(col("PARTITION_DATE") > lastPartitionDate)
      .withColumn("max_date", max("PARTITION_DATE").over())
      .filter(col("PARTITION_DATE") === col("max_date")).drop("max_date")
  }

  override def write(inputDf: DataFrame, mode: SaveMode): Unit = super.write(inputDf, Overwrite)

  private def getLastPartitionDate: String = {
    val currentPartitionPath = os.list(getOsPath(ingestionPath.outputPath)).filter(os.isDir(_))
    if (currentPartitionPath.nonEmpty) currentPartitionPath.map(f => f.baseName.split("=")(1)).max else "0"
  }
}
