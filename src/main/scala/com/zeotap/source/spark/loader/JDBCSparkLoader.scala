package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF
import com.zeotap.common.types.{JDBC, OptionalColumn, SupportedFeaturesF}
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.source.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class JDBCSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(JDBC)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) {

  /**
   * This function can be used to provide all the options required by Spark to read from JDBC
   */
  def connectionProperties(url: String, user: String, password: String, tableName: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesF.connectionProperties(url, user, password, tableName), readerToDataFrameProperties, dataFrameProperties)

  /**
   * The custom schema to use for reading data from JDBC connectors. For example, "id DECIMAL(38, 0), name STRING".
   * You can also specify partial fields, and the others use the default type mapping
   * i.e., all columns are returned but with type casting if provided in the custom schema
   * For example, "id DECIMAL(38, 0)". The column names should be identical to the corresponding column names of JDBC table.
   * Users can specify the corresponding data types of Spark SQL instead of using the defaults.
   */
  def customSchema(schema: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesF.customSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load(): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.load(), dataFrameProperties)

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.addOptionalColumns(columns))

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
