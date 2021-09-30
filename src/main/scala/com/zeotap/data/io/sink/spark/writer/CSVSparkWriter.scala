package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{CSV, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class CSVSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(CSV)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends FSSparkWriter(writerProperties, writerToSinkProperties) {

  /**
   * Custom separator/delimiter can be provided for the CSV file
   */
  def separator(separator: String): CSVSparkWriter =
    CSVSparkWriter(writerProperties :+ SupportedFeaturesHelper.separator(separator), writerToSinkProperties)

  /**
   * Schema for each column is inferred by Spark internally for the CSV dataset
   */
  def inferSchema: CSVSparkWriter =
    CSVSparkWriter(writerProperties :+ SupportedFeaturesHelper.inferSchema, writerToSinkProperties)

  /**
   * If the first row of the CSV dataset denotes the headers for the columns,
   * this option can be provided to write the CSV with column names
   */
  def header: CSVSparkWriter =
    CSVSparkWriter(writerProperties :+ SupportedFeaturesHelper.header, writerToSinkProperties)

}
