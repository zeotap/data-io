package com.zeotap.sink.spark.writer

import com.zeotap.common.types.{SinkWriter, SupportedFeaturesHelper}
import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.sink.utils.SparkWriterUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

object SparkWriter {

  def text: TextSparkWriter = TextSparkWriter()

  def csv: CSVSparkWriter = CSVSparkWriter()

  def json: JSONSparkWriter = JSONSparkWriter()

  def avro: AvroSparkWriter = AvroSparkWriter()

  def parquet: ParquetSparkWriter = ParquetSparkWriter()

  def orc: ORCSparkWriter = ORCSparkWriter()

  def jdbc: JDBCSparkWriter = JDBCSparkWriter()

}

class SparkWriter(
 writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(),
 writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends SinkWriter {

  /**
   * Saves `DataFrame` as an output, for data sinks that don't require a path (e.g. external
   * key-value stores).
   */
  def save(): SparkWriter =
    new SparkWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.save())

  def buildUnsafe(implicit dataFrame: DataFrame): Unit =
    SparkWriterUtils.buildWriter(writerProperties, writerToSinkProperties)

  def buildSafe(implicit dataFrame: DataFrame): Either[String, Unit] = handleException(buildUnsafe)

}
