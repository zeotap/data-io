package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{JSON, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class JSONSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(JSON)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the JSON file has the JSON objects written in a multi-line fashion, this option needs to be provided
   */
  def multiLine: JSONSparkLoader =
    JSONSparkLoader(readerProperties :+ SupportedFeaturesF.multiLine, readerToDataFrameProperties, dataFrameProperties)
}
