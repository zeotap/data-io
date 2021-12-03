package com.zeotap.data.io.sink.beam.writer

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JDBC, SupportedFeaturesHelper}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter

case class JDBCBeamWriter(
  writerProperties: Seq[SupportedFeaturesF[PCollectionWriter]] = Seq(SupportedFeaturesHelper.addFormat(JDBC)),
  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]] = Seq()
) extends BeamWriter(writerProperties, writerToSinkProperties) {

  /**
   * Provides custom schema to the Beam PCollectionWriter
   * @param jsonSchema The GenericRecord schema needs to be provided as a jsonString
   */
  def schema(jsonSchema: String): JDBCBeamWriter =
    JDBCBeamWriter(writerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), writerToSinkProperties)

  /**
   * This function can be used to provide all the options required by Beam to write to JDBC
   */
  def connectionProperties(url: String, user: String, password: String): JDBCBeamWriter =
    JDBCBeamWriter(writerProperties :+ SupportedFeaturesHelper.connectionProperties(url, user, password), writerToSinkProperties)

  /**
   * This function can be used to provide the JDBC driver class name required by Beam to write to JDBC
   */
  def driver(driver: String): JDBCBeamWriter =
    JDBCBeamWriter(writerProperties :+ SupportedFeaturesHelper.driver(driver), writerToSinkProperties)

  /**
   * Provides the tableName required by Beam to write to JDBC
   */
  def tableName(tableName: String): JDBCBeamWriter =
    JDBCBeamWriter(writerProperties :+ SupportedFeaturesHelper.tableName(tableName), writerToSinkProperties)

}
