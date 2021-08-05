package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF
import com.zeotap.common.types.{JSON, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class JSONSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(JSON)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the JSON file has the JSON objects written in a multi-line fashion, this option needs to be provided
   */
  def multiLine: JSONSparkLoader =
    JSONSparkLoader(readerProperties :+ SupportedFeaturesF.multiLine, readerToDataFrameProperties, dataFrameProperties)
}
