import os.{Path, SubPath}

case class PathHelper(
                       inputPath: String,
                       schemaPath: String,
                       outputPath: String
                     )

object PathHelper {
  val transactionsPath: PathHelper = PathHelper(inputPath = "data/inputs/transactions_", schemaPath = "metadata/transactions.ddl", outputPath = "data/registry/transactions")
  val receiversPath: PathHelper = PathHelper(inputPath = "data/inputs/receivers_registry_", schemaPath = "metadata/receivers_registry.ddl", outputPath = "data/registry/receivers")
  val exchangeRatesPath: PathHelper = PathHelper(inputPath = "data/inputs/exchange_rates_registry_", schemaPath = "metadata/exchange_rates_registry.ddl", outputPath = "data/registry/exchange_rates")

  def getInputCsvFilePath(path: PathHelper, date: String) = s"${path.inputPath}$date.csv"

  def getOsPath(directory: String): Path = os.pwd / SubPath(directory)
}
