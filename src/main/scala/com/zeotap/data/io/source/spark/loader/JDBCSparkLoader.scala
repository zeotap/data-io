package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JDBC, SupportedFeaturesHelper}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class JDBCSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(JDBC)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends SparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * This function can be used to provide all the options required by Spark to read from JDBC
   */
  def connectionProperties(url: String, user: String, password: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesHelper.connectionProperties(url, user, password), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Provides the tableName required by Spark to read from JDBC. Cannot be used along with query
   */
  def tableName(tableName: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesHelper.tableName(tableName), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Provides a custom query to Spark for loading from JDBC. Example: "select c1, c2 from t1". Cannot be used along with tableName
   */
  def query(query: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesHelper.query(query), readerToDataFrameProperties, dataFrameProperties)

  /**
   * The custom schema to use for reading data from JDBC connectors. For example, "id DECIMAL(38, 0), name STRING".
   * You can also specify partial fields, and the others use the default type mapping
   * i.e., all columns are returned but with type casting if provided in the custom schema
   * For example, "id DECIMAL(38, 0)". The column names should be identical to the corresponding column names of JDBC table.
   * Users can specify the corresponding data types of Spark SQL instead of using the defaults.
   */
  def customSchema(schema: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesHelper.customSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load(): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.load(), dataFrameProperties)

}
