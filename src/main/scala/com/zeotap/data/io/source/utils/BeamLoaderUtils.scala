package com.zeotap.data.io.source.utils

import com.zeotap.data.io.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import com.zeotap.data.io.source.beam.interpreters.BeamInterpreters._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, Row}

object BeamLoaderUtils {

  def buildLoader(readerProperties: Seq[SupportedFeaturesF[PCollectionReader]],
                  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[Row]]])(implicit beam: Pipeline): PCollection[Row] = {
    val reader = featuresCompiler(readerProperties).foldMap[BeamReaderState](readerInterpreter).run(PCollectionReader(Map())).value._1
    featuresCompiler(readerToPCollectionProperties).foldMap[BeamReader](readerToPCollectionInterpreter).run(reader)
  }

}
