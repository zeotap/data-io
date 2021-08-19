package com.zeotap.sink.spark.writer

import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{JDBC, SaveMode, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameWriter

case class JDBCSparkWriter(
  writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]] = Seq(SupportedFeaturesHelper.addFormat(JDBC)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends SparkWriter(writerProperties, writerToSinkProperties) {

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
   * @param saveMode Supported formats = {Append, ErrorIfExists, Ignore, Overwrite}
   */
  def addSaveMode(saveMode: SaveMode): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.addSaveMode(saveMode), writerToSinkProperties)

}
