package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{CSV, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class CSVBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(CSV)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSBeamWriter(writerProperties, writerToSinkProperties) {

  /**
   * Custom separator/delimiter can be provided for the CSV file
   */
  def separator(separator: String): CSVBeamWriter =
    CSVBeamWriter(writerProperties :+ SupportedFeaturesHelper.separator(separator), writerToSinkProperties)

}
