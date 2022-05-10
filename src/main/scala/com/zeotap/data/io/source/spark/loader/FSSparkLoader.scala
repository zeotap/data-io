package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{DataFormatType, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

class FSSparkLoader[A](readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(),
                       readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
                       dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
                      ) extends SparkLoader[A](readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Provides custom schema to the Spark DataFrameReader
   *
   * @param jsonSchema The StructType schema needs to be provided as a jsonString (jsonSchema = schema.json)
   */
  def schema(jsonSchema: String) =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Adds the input format to the Spark DataFrameReader
   *
   * @param format Supported formats = {Text, CSV, JSON, Avro, Parquet, ORC}
   */
  def addFormat(format: DataFormatType) =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.addFormat(format), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Sets the base path that is needed for partition discovery
   */
  def basePath(path: String) =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesHelper.basePath(path), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load() =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.load(), dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String) =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPath(path), dataFrameProperties)

  /**
   * Loads input from multiple paths as a single `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(paths: List[String]) =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPaths(paths), dataFrameProperties)

  /**
   * Look-back operation performed to obtain all available paths before the given date and within the lookBackWindow.
   * All the obtained paths are loaded as a single `DataFrame` and returned.
   *
   * @param parameters   should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer) =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.lookBack(pathTemplate, parameters, lookBackWindow), dataFrameProperties)

  /**
   * The latest paths (sub-folders for a given pathTemplate) with respect to the given date are obtained and loaded into a `DataFrame`.
   * If relativeToCurrentDate is set to true, the obtained paths will be of a date prior to the provided date and
   * if it is set to false, the latest paths available in the fileSystem will be returned.
   *
   * @param parameters   should contain values for YR, MON, DT. HR and MIN are optional.
   * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
   */
  // Another example for pathTemplate: gs://bucket/*/yr=${YR}/mon=${MON}/dt=${DT}, this would return the latest paths for
  // gs://bucket/path1/yr=${YR}/mon=${MON}/dt=${DT}, gs://bucket/path2/yr=${YR}/mon=${MON}/dt=${DT}, etc.
  def latestPaths(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean) =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.latestPaths(pathTemplate, parameters, relativeToCurrentDate), dataFrameProperties)

  /**
   * Adds a column based on the provided operation
   *
   * @param operation needs to be one of `addColumn`, `renameColumn`
   *                  case `addColumn` => outputTsColumn calculated from the flat files' create TS
   *                  case `renameColumn` => existing timestamp column value used for outputTsColumn
   */
  def addCreationTimestamp(operation: String, inputColumn: Option[String], outputColumn: String) =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addCreationTimestamp(operation, inputColumn, outputColumn))

}


