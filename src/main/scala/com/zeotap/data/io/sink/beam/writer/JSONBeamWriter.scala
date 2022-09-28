package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JSON, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class JSONBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(JSON)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSBeamWriter(writerProperties, writerToSinkProperties) {

}
