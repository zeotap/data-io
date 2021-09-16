package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SupportedFeaturesHelper, Text}
import org.apache.spark.sql.DataFrameWriter

case class TextSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

  /**
   * Custom separator/delimiter can be provided for the Text file
   */
  def separator(separator: String): TextSparkWriter =
    TextSparkWriter(writerProperties :+ SupportedFeaturesHelper.separator(separator), writerToSinkProperties)

  /**
   * Schema for each column is inferred by Spark internally for the Text dataset
   */
  def inferSchema: TextSparkWriter =
    TextSparkWriter(writerProperties :+ SupportedFeaturesHelper.inferSchema, writerToSinkProperties)

  /**
   * If the first row of the Text dataset denotes the headers for the columns,
   * this option can be provided to write the Text file with column names
   */
  def header: TextSparkWriter =
    TextSparkWriter(writerProperties :+ SupportedFeaturesHelper.header, writerToSinkProperties)

}
