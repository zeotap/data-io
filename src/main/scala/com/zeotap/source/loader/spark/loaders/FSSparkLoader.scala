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

  def addFormat(format: DataFormatType): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesF.addFormat(format), readerToDataFrameProperties, dataFrameProperties)

  def basePath(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties :+ SupportedFeaturesF.basePath(path), dataFrameProperties)

  def load(): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.load(), dataFrameProperties)

  def load(path: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.loadPath(path), dataFrameProperties)

  def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.lookBack(pathTemplate, parameters, lookBackWindow), dataFrameProperties)

  def latestPath(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesF.latestPath(pathTemplate, parameters, relativeToCurrentDate), dataFrameProperties)

  def optionalColumns(columns: List[OptionalColumn]): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.optionalColumns(columns))

  def addCreationTimestamp(inputType: String): FSSparkLoader =
    new FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesF.addCreationTimestamp(inputType))

  def build(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  def buildWithExceptionHandling(implicit spark: SparkSession): Either[String, DataFrame] = handleException(build)

}


