package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{Avro, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class AvroSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(Avro)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

}
