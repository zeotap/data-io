package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types._
import com.zeotap.source.loader.utils.CommonUtils.handleException
import com.zeotap.source.loader.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class FSSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) {

  /**
   * Adds the input format to the Spark DataFrameReader
   * @param format Supported formats = {TEXT, CSV, JSON, AVRO, PARQUET, ORC}
   */
  def addFormat(format: DataFormatType): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesF.addFormat(format), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Sets the base path that is needed for partition discovery
   */
  def basePath(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesF.basePath(path), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load(): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.load(), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.loadPath(path), dataFrameProperties)

  /**
   * Look-back operation performed to obtain all available paths before the given date and within the lookBackWindow.
   * All the obtained paths are loaded as a single `DataFrame` and returned.
   * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.lookBack(pathTemplate, parameters, lookBackWindow), dataFrameProperties)

  /**
   * The latest path with respect to the given date is obtained and loaded into a `DataFrame`.
   * If relativeToCurrentDate is set to true, the obtained path will be of a date prior to the provided date and
   * if it is set to false, the latest path available in the fileSystem will be returned.
   * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  def latestPath(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.latestPath(pathTemplate, parameters, relativeToCurrentDate), dataFrameProperties)

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.addOptionalColumns(columns))

  /**
   * Adds a column `CREATED_TS_raw` based on the provided inputType
   * @param inputType needs to be one of `raw`, `tube`, `preprocess`
   * case `raw` => GCS flat files (CREATED_TS_raw calculated from the part files' create TS)
   * case `tube` => SmartPixel/Tube data (Existing timestamp column value used for CREATED_TS_raw)
   * case `preprocess` => Preprocessed data (CREATED_TS_raw column is expected to be present in the inputData)
   */
  def addCreationTimestamp(inputType: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.addCreationTimestamp(inputType))

  /**
   * Returns a `DataFrame` based on all the provided reader and dataFrame properties
   */
  def build(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  /**
   * Exception-safe build function to return either exception message or `DataFrame`
   */
  def buildWithExceptionHandling(implicit spark: SparkSession): Either[String, DataFrame] = handleException(build)

}


