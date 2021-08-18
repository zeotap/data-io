package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{SupportedFeaturesHelper, Text}
import org.apache.spark.sql.DataFrameWriter

case class TextSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

}
