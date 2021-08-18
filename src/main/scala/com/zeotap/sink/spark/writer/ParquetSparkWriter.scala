package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{Parquet, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class ParquetSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(Parquet)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

}
