import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.flatspec.AnyFlatSpec

class GuaranteesReportTest extends AnyFlatSpec with SparkSessionTestWrapper with DatasetComparer {
  it should "Returns expected guarantees report" in {
    import spark.implicits._
    val expectedDf = List(
      ("G_001", 600, 4000, 0, 0, 200, 3000, 0, 0, "ES", "25.01.2024"),
      ("G_002", 0, 100, 0, 0, 0, 200, 3000, 0, 0, "ES", "25.01.2024"),
      ("G_004", 0, 4000, 0, 0, 200, 3000, 0, 0, "ES", "25.01.2024"),
      ("G_005", 2000, 0, 0, 3000, 1440, 0, 0, 900, "US", "25.01.2024"),
      ("G_006", 1400, 0, 0, 0, 1440, 0, 0, 900, "US", "25.01.2024"),
      ("G_007", 1400, 0, 0, 0, 1440, 0, 0, 900, "US", "25.01.2024")
    ).toDF("GUARANTOR_NAME", "CLASS_A", "CLASS_B", "CLASS_C", "CLASS_D", "AVG_CLASS_A", "AVG_CLASS_B", "AVG_CLASS_C", "AVG_CLASS_D", "COUNTRY", "PARTITION_DATE")

    val guaranteesReport = new GuaranteesReport(spark, "25.01.2024")
    val inputDf = guaranteesReport.read()
    val result = guaranteesReport.transform(inputDf)
    assertSmallDatasetEquality(result, expectedDf)
  }
}
