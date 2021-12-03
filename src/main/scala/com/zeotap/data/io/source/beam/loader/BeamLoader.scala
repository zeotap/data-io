package com.zeotap.data.io.source.beam.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{OptionalColumn, SourceLoader, SupportedFeaturesHelper}
import com.zeotap.data.io.common.utils.CommonUtils.handleException
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import com.zeotap.data.io.source.utils.BeamLoaderUtils
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection

object BeamLoader {

  def text: TextBeamLoader = TextBeamLoader()

  def csv: CSVBeamLoader = CSVBeamLoader()

  def json: JSONBeamLoader = JSONBeamLoader()

  def avro: AvroBeamLoader = AvroBeamLoader()

  def parquet: ParquetBeamLoader = ParquetBeamLoader()

  def jdbc: JDBCBeamLoader = JDBCBeamLoader()

}

class BeamLoader(
  readerProperties: Seq[SupportedFeaturesF[PCollectionReader]] = Seq(),
  readerToPCollectionProperties: Seq[SupportedFeaturesF[PCollection[GenericRecord]]] = Seq()
) extends SourceLoader {

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): BeamLoader =
    new BeamLoader(readerProperties :+ SupportedFeaturesHelper.addOptionalColumns(columns), readerToPCollectionProperties)

  /**
   * Returns a `PCollection[GenericRecord]` based on all the provided reader and pCollection properties
   */
  def buildUnsafe(implicit beam: Pipeline): PCollection[GenericRecord] =
    BeamLoaderUtils.buildLoader(readerProperties, readerToPCollectionProperties)

  /**
   * Exception-safe build function to return either exception message or `PCollection[GenericRecord]`
   */
  def buildSafe(implicit beam: Pipeline): Either[String, PCollection[GenericRecord]] = handleException(buildUnsafe)

}
