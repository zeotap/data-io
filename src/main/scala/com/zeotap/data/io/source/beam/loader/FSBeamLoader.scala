package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{DataFormatType, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.beam.sdk.values.{PCollection, Row}

class FSBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[Row]]] = Seq()
) extends BeamLoader(readerProperties, readerToPCollectionProperties) {

  /**
   * Adds the input format to the Beam PCollectionReader
   * @param format Supported formats = {Text, CSV, JSON, Avro, Parquet}
   */
  def addFormat(format: DataFormatType): FSBeamLoader =
    new FSBeamLoader(readerProperties :+ SupportedFeaturesHelper.addFormat(format), readerToPCollectionProperties)

  /**
   * Provides custom schema to the Beam PCollectionReader
   * @param jsonSchema The GenericRecord schema needs to be provided as a jsonString
   */
  def schema(jsonSchema: String): FSBeamLoader =
  new FSBeamLoader(readerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), readerToPCollectionProperties)

  /**
   * Loads input in as a `PCollection[GenericRecord]`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String): FSBeamLoader =
    new FSBeamLoader(readerProperties, readerToPCollectionProperties :+ SupportedFeaturesHelper.loadPath(path))

}
