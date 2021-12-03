package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Avro, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

case class AvroBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(Avro)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {

}
