package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{CSV, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class CSVSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(CSV)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  def separator(separator: String): CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.separator(separator), readerToDataFrameProperties, dataFrameProperties)

  def inferSchema: CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.inferSchema, readerToDataFrameProperties, dataFrameProperties)

  def header: CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.header, readerToDataFrameProperties, dataFrameProperties)
}
