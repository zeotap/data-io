package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{CSV, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

case class CSVBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(CSV)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {

  /**
   * Custom separator/delimiter can be provided for the CSV file
   */
  def separator(separator: String): CSVBeamLoader =
    CSVBeamLoader(readerProperties :+ SupportedFeaturesHelper.separator(separator), readerToPCollectionProperties)

}
