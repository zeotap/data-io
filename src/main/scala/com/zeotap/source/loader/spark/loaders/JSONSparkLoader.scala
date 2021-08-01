package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{JSON, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class JSONSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(JSON)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  def multiLine: JSONSparkLoader =
    JSONSparkLoader(readerProperties :+ SupportedFeaturesF.multiLine, readerToDataFrameProperties, dataFrameProperties)
}
