package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{JDBC, OptionalColumn, SupportedFeaturesF}
import com.zeotap.source.loader.utils.CommonUtils.handleException
import com.zeotap.source.loader.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class JDBCSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(JDBC)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) {

  def connectionProperties(url: String, user: String, password: String, tableName: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesF.connectionProperties(url, user, password, tableName), readerToDataFrameProperties, dataFrameProperties)

  def customSchema(schema: String): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties :+ SupportedFeaturesF.customSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  def load(): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.load(), dataFrameProperties)

  def optionalColumns(columns: List[OptionalColumn]): JDBCSparkLoader =
    JDBCSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.optionalColumns(columns))

  def build(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  def buildWithExceptionHandling(implicit spark: SparkSession): Either[String, DataFrame] = handleException(build)

}
