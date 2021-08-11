package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{DataFormatType, SaveMode, SupportedFeaturesHelper}
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.sink.utils.SparkWriterUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

class FSSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) {

  /**
   * Adds the output format to the Spark DataFrameWriter
   * @param format Supported formats = {TEXT, CSV, JSON, AVRO, PARQUET, ORC}
   */
  def addFormat(format: DataFormatType): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.addFormat(format), writerToSinkProperties)

  /**
   * Adds the saveMode to the Spark DataFrameWriter
   * @param saveMode Supported formats = {APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
   */
  def addSaveMode(saveMode: SaveMode): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.addSaveMode(saveMode), writerToSinkProperties)

  /**
   * Used to write data into sub-folders on the basis of columnName and columnValue
   */
  def partitionBy(columnNames: List[String]): FSSparkWriter =
    new FSSparkWriter(writerProperties :+ SupportedFeaturesHelper.partitionBy(columnNames), writerToSinkProperties)

  /**
   * Saves `DataFrame` as an output, for data sinks that don't require a path (e.g. external
   * key-value stores).
   */
  def save(): FSSparkWriter =
    new FSSparkWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.save())

  /**
   * Saves `DataFrame` as an output, for data sinks that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def save(path: String): FSSparkWriter =
    new FSSparkWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.saveToPath(path))

  def buildUnsafe(implicit dataFrame: DataFrame): Unit =
    SparkWriterUtils.buildWriter(writerProperties, writerToSinkProperties)

  def buildSafe(implicit dataFrame: DataFrame): Either[String, Unit] = handleException(buildUnsafe)

}
