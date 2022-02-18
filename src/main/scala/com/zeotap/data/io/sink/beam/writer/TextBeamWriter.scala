package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SupportedFeaturesHelper, Text}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class TextBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSBeamWriter(writerProperties, writerToSinkProperties) {

  /**
   * Custom separator/delimiter can be provided for the Text file
   */
  def separator(separator: String): TextBeamWriter =
    TextBeamWriter(writerProperties :+ SupportedFeaturesHelper.separator(separator), writerToSinkProperties)

}
