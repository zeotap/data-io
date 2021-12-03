package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SupportedFeaturesHelper, Text}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

case class TextBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {
  /**
   * Custom separator/delimiter can be provided for the Text file
   */
  def separator(separator: String): TextBeamLoader =
    TextBeamLoader(readerProperties :+ SupportedFeaturesHelper.separator(separator), readerToPCollectionProperties)

}
