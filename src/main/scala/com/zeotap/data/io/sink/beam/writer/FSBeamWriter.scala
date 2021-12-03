package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{DataFormatType, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

class FSBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends BeamWriter(writerProperties, writerToSinkProperties) {

  /**
   * Adds the output format to the Beam PCollectionWriter
   * @param format Supported formats = {Text, CSV, JSON, Avro, Parquet, ORC}
   */
  def addFormat(format: DataFormatType): FSBeamWriter =
    new FSBeamWriter(writerProperties :+ SupportedFeaturesHelper.addFormat(format), writerToSinkProperties)

  /**
   * Provides custom schema to the Beam PCollectionWriter
   * @param jsonSchema The GenericRecord schema needs to be provided as a jsonString
   */
  def schema(jsonSchema: String): FSBeamWriter =
    new FSBeamWriter(writerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), writerToSinkProperties)

  /**
   * Saves `PCollection[GenericRecord]` as an output, for data sinks that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def save(path: String): FSBeamWriter =
    new FSBeamWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.saveToPath(path))

}
