package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{JDBC, SaveMode, SupportedFeaturesHelper}
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.sink.utils.SparkWriterUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

case class JDBCSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(JDBC)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) {

  /**
   * This function can be used to provide all the options required by Spark to write to JDBC
   */
  def connectionProperties(url: String, user: String, password: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.connectionProperties(url, user, password), writerToSinkProperties)

  /**
   * Provides the tableName required by Spark to write to JDBC
   */
  def tableName(tableName: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.tableName(tableName), writerToSinkProperties)

  /**
   * Adds the saveMode to the Spark DataFrameWriter
   * @param saveMode Supported formats = {APPEND, ERROR_IF_EXISTS, IGNORE, OVERWRITE}
   */
  def addSaveMode(saveMode: SaveMode): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.addSaveMode(saveMode), writerToSinkProperties)

  /**
   * Saves `DataFrame` as an output, for data sinks that don't require a path (e.g. external
   * key-value stores).
   */
  def save(): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties, writerToSinkProperties :+ SupportedFeaturesHelper.save())

  def buildUnsafe(implicit dataFrame: DataFrame): Unit =
    SparkWriterUtils.buildWriter(writerProperties, writerToSinkProperties)

  def buildSafe(implicit dataFrame: DataFrame): Either[String, Unit] = handleException(buildUnsafe)

}
