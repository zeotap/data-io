package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JSON, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class JSONSparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(JSON)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the JSON file has the JSON objects written in a multi-line fashion, this option needs to be provided
   */
  def multiLine =
    JSONSparkLoader(readerProperties :+ SupportedFeaturesHelper.multiLine, readerToDataFrameProperties, dataFrameProperties)
}
