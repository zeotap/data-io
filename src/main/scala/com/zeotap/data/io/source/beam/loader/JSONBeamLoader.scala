package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JSON, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

case class JSONBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(JSON)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]] = Seq()
) extends FSBeamLoader(readerProperties, readerToPCollectionProperties) {

}
