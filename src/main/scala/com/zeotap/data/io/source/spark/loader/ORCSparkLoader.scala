package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{ORC, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class ORCSparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(ORC)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

}
