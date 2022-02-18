package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{JDBC, SupportedFeaturesHelper}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import org.apache.beam.sdk.values.{PCollection, Row}

case class JDBCBeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(SupportedFeaturesHelper.addFormat(JDBC)),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[Row]]] = Seq()
) extends BeamLoader(readerProperties, readerToPCollectionProperties) {

  /**
   * Provides custom schema to the Beam PCollectionReader
   * @param jsonSchema The GenericRecord schema needs to be provided as a jsonString
   */
  def schema(jsonSchema: String): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties :+ SupportedFeaturesHelper.schema(jsonSchema), readerToPCollectionProperties)

  /**
   * This function can be used to provide all the options required by Beam to read from JDBC
   */
  def connectionProperties(url: String, user: String, password: String): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties :+ SupportedFeaturesHelper.connectionProperties(url, user, password), readerToPCollectionProperties)

  /**
   * This function can be used to provide the JDBC driver class name required by Beam to read from JDBC
   */
  def driver(driver: String): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties :+ SupportedFeaturesHelper.driver(driver), readerToPCollectionProperties)

  /**
   * Provides the tableName required by Beam to read from JDBC. Cannot be used along with query
   */
  def tableName(tableName: String): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties :+ SupportedFeaturesHelper.tableName(tableName), readerToPCollectionProperties)

  /**
   * Provides a custom query to Beam for loading from JDBC. Example: "select c1, c2 from t1". Cannot be used along with tableName
   */
  def query(query: String): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties :+ SupportedFeaturesHelper.query(query), readerToPCollectionProperties)

  /**
   * Loads input in as a `PCollection[GenericRecord]`, for data sources that don't require a path (e.g. external
   * key-value stores).
   */
  def load(): JDBCBeamLoader =
    JDBCBeamLoader(readerProperties, readerToPCollectionProperties :+ SupportedFeaturesHelper.load())

}
