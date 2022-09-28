package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Avro, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.beam.sdk.values.{PCollection, Row}

case class AvroBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(Avro)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[Row]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {

}
