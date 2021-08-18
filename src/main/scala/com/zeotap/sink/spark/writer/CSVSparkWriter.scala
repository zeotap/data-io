package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{CSV, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class CSVSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(CSV)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

}