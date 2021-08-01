package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{BIGQUERY, OptionalColumn, SupportedFeaturesF}
import com.zeotap.source.loader.utils.CommonUtils.handleException
import com.zeotap.source.loader.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class BigQuerySparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(BIGQUERY)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) {

  def load(path: String): BigQuerySparkLoader =
    BigQuerySparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.loadPath(path), dataFrameProperties)

  def optionalColumns(columns: List[OptionalColumn]): BigQuerySparkLoader =
    BigQuerySparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.optionalColumns(columns))

  def build(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  def buildWithExceptionHandling(implicit spark: SparkSession): Either[String, DataFrame] = handleException(build)
}
