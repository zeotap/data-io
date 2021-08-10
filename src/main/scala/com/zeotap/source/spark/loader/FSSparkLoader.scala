package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{DataFormatType, OptionalColumn, SupportedFeaturesHelper}
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.source.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class FSSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) {

  /**
   * Provides custom schema to the Spark DataFrameReader
   * @param jsonSchema The StructType schema needs to be provided as a jsonString (jsonSchema = schema.json)
   */
  def schema(jsonSchema: String): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Adds the input format to the Spark DataFrameReader
   * @param format Supported formats = {TEXT, CSV, JSON, AVRO, PARQUET, ORC}
   */
  def addFormat(format: DataFormatType): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.addFormat(format), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Sets the base path that is needed for partition discovery
   */
  def basePath(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.basePath(path), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load(): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.load(), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPath(path), dataFrameProperties)

  /**
   * Loads input from multiple paths as a single `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(paths: List[String]): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPaths(paths), dataFrameProperties)

  /**
   * Look-back operation performed to obtain all available paths before the given date and within the lookBackWindow.
   * All the obtained paths are loaded as a single `DataFrame` and returned.
   * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.lookBack(pathTemplate, parameters, lookBackWindow), dataFrameProperties)

  /**
   * The latest path with respect to the given date is obtained and loaded into a `DataFrame`.
   * If relativeToCurrentDate is set to true, the obtained path will be of a date prior to the provided date and
   * if it is set to false, the latest path available in the fileSystem will be returned.
   * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  def latestPath(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.latestPath(pathTemplate, parameters, relativeToCurrentDate), dataFrameProperties)

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addOptionalColumns(columns))

  /**
   * Adds a column based on the provided operation
   * @param operation needs to be one of `addColumn`, `renameColumn`
   * case `addColumn` => outputTsColumn calculated from the flat files' create TS
   * case `renameColumn` => existing timestamp column value used for outputTsColumn
   */
  def addCreationTimestamp(operation: String, inputColumn: Option[String], outputColumn: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addCreationTimestamp(operation, inputColumn, outputColumn))

  /**
   * Returns a `DataFrame` based on all the provided reader and dataFrame properties
   */
  def buildUnsafe(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  /**
   * Exception-safe build function to return either exception message or `DataFrame`
   */
  def buildSafe(implicit spark: SparkSession): Either[String, DataFrame] = handleException(buildUnsafe)

}


