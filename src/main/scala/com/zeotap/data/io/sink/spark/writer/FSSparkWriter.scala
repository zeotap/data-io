package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{DataFormatType, SaveMode, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

class FSSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends SparkWriter(writerProperties, writerToSinkProperties) {

  /**
   * Adds the output format to the Spark DataFrameWriter
   * @param format Supported formats = {Text, CSV, JSON, Avro, Parquet, ORC}
   */
  def addFormat(format: DataFormatType): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.addFormat(format), writerToSinkProperties)

  /**
   * Adds the saveMode to the Spark DataFrameWriter
   * @param saveMode Supported formats = {Append, ErrorIfExists, Ignore, Overwrite}
   */
  def addSaveMode(saveMode: SaveMode): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.addSaveMode(saveMode), writerToSinkProperties)

  /**
   * Used to write data into sub-folders on the basis of columnName and columnValue
   */
  def partitionBy(columnNames: List[String]): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.partitionBy(columnNames), writerToSinkProperties)

  /**
   * Saves `DataFrame` as an output, for data sinks that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def save(path: String): FSSparkWriter =
    new FSSparkWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.saveToPath(path))

}
