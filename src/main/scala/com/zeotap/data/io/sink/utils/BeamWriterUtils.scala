package com.zeotap.data.io.sink.utils

import com.zeotap.data.io.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter
import com.zeotap.data.io.sink.beam.interpreters.BeamInterpreters._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

object BeamWriterUtils {

  def buildWriter(writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]],
                  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]])(implicit pCollection: PCollection[GenericRecord]): Unit = {
    val writer = featuresCompiler(writerProperties).foldMap[BeamWriterState](writerInterpreter).run(PCollectionWriter(pCollection, Map())).value._1
    featuresCompiler(writerToSinkProperties).foldMap[BeamWriter](writerToSinkInterpreter).run(writer)
  }

}
