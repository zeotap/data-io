package com.zeotap.data.io.source.utils

import com.zeotap.data.io.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import com.zeotap.data.io.source.beam.interpreters.BeamInterpreters.{BeamReader, _}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection

object BeamLoaderUtils {

  def buildLoader(readerProperties: Seq[SupportedFeaturesF[PCollectionReader]],
                  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]])(implicit beam: Pipeline): PCollection[GenericRecord] = {
    val reader = featuresCompiler(readerProperties).foldMap[BeamReaderState](readerInterpreter).run(PCollectionReader(Map())).value._1
    featuresCompiler(readerToPCollectionProperties).foldMap[BeamReader](readerToPCollectionInterpreter).run(reader)
  }

}
