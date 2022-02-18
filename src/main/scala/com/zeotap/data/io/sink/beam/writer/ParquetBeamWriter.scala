package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Parquet, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class ParquetBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(Parquet)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSBeamWriter(writerProperties, writerToSinkProperties) {

}
