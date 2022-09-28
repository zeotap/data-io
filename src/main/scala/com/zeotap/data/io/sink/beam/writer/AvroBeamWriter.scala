package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Avro, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class AvroBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(Avro)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSBeamWriter(writerProperties, writerToSinkProperties) {

}
