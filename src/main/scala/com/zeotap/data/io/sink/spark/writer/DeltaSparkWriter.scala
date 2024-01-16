package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.{Delta, SupportedFeaturesHelper}
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import org.apache.spark.sql.DataFrameWriter

case class DeltaSparkWriter(
    writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(Delta)),
    writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

}
