package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SupportedFeaturesHelper, Text}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.beam.sdk.values.{PCollection, Row}

case class TextBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[Row]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {
  /**
   * Custom separator/delimiter can be provided for the Text file
   */
  def separator(separator: String): TextBeamLoader =
    TextBeamLoader(readerProperties :+ SupportedFeaturesHelper.separator(separator), readerToPCollectionProperties)

}
