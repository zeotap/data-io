package com.zeotap.data.io.sink.spark.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JDBC, SaveMode, SupportedFeaturesHelper}
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
   * This function can be used to provide the JDBC driver class name required by Spark to write to JDBC
   */
  def driver(driver: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.driver(driver), writerToSinkProperties)

  /**
   * Provides the tableName required by Spark to write to JDBC
   */
  def tableName(tableName: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.tableName(tableName), writerToSinkProperties)

  /**
   * Provides the stringtype mapping required by Spark to write to JDBC
   */
  def stringType(stringType: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.stringType(stringType), writerToSinkProperties)

  /**
   * Adds the saveMode to the Spark DataFrameWriter
   * @param saveMode Supported formats = {Append, ErrorIfExists, Ignore, Overwrite}
   */
  def addSaveMode(saveMode: SaveMode): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.addSaveMode(saveMode), writerToSinkProperties)

  def batchSize(batchSize: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.batchSize(batchSize), writerToSinkProperties)

  def numPartitions(numPartitions: String): JDBCSparkWriter =
    JDBCSparkWriter(writerProperties :+ SupportedFeaturesHelper.batchSize(numPartitions), writerToSinkProperties)

}
