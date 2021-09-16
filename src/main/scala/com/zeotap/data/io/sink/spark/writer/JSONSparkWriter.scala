package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JSON, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class JSONSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(JSON)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

  /**
   * This option can be provided if compression is required
   */
  def compression(compression: String): JSONSparkWriter =
    JSONSparkWriter(writerProperties :+ SupportedFeaturesHelper.compression(compression), writerToSinkProperties)

}
