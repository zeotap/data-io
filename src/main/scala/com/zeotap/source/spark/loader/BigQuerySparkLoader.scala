package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{BIGQUERY, OptionalColumn, SupportedFeaturesHelper}
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.source.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class BigQuerySparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(BIGQUERY)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) {

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String): BigQuerySparkLoader =
    BigQuerySparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPath(path), dataFrameProperties)

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): BigQuerySparkLoader =
    BigQuerySparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addOptionalColumns(columns))

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
