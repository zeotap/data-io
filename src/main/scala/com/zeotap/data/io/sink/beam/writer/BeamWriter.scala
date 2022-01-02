package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SinkWriter, SupportedFeaturesHelper}
import com.zeotap.data.io.common.utils.CommonUtils.handleException
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter
import com.zeotap.data.io.sink.utils.BeamWriterUtils
import org.apache.beam.sdk.values.{PCollection, Row}

object BeamWriter {

  def text: TextBeamWriter = TextBeamWriter()

  def csv: CSVBeamWriter = CSVBeamWriter()

  def json: JSONBeamWriter = JSONBeamWriter()

  def avro: AvroBeamWriter = AvroBeamWriter()

  def parquet: ParquetBeamWriter = ParquetBeamWriter()

  def jdbc: JDBCBeamWriter = JDBCBeamWriter()

}

class BeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends SinkWriter {

  /**
   * Saves `PCollection[GenericRecord]` as an output, for data sinks that don't require a path (e.g. external
   * key-value stores).
   */
  def save(): BeamWriter =
  new BeamWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.save())

  def buildUnsafe(implicit pCollection: PCollection[Row]): Unit =
    BeamWriterUtils.buildWriter(writerProperties, writerToSinkProperties)

  def buildSafe(implicit pCollection: PCollection[Row]): Either[String, Unit] = handleException(buildUnsafe)

}
