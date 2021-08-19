package com.zeotap.source.spark.loader

import com.zeotap.common.types.{OptionalColumn, SourceLoader, SupportedFeaturesHelper}
import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.utils.CommonUtils.handleException
import com.zeotap.source.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object SparkLoader {

  def text: TextSparkLoader = TextSparkLoader()

  def csv: CSVSparkLoader = CSVSparkLoader()

  def json: JSONSparkLoader = JSONSparkLoader()

  def avro: AvroSparkLoader = AvroSparkLoader()

  def parquet: ParquetSparkLoader = ParquetSparkLoader()

  def orc: ORCSparkLoader = ORCSparkLoader()

  def jdbc: JDBCSparkLoader = JDBCSparkLoader()

  def bigquery: BigQuerySparkLoader = BigQuerySparkLoader()

}

class SparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends SourceLoader {

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]): SparkLoader =
    new SparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addOptionalColumns(columns))

  /**
   * Returns a `DataFrame` based on all the provided reader and dataFrame properties
   */
  def buildUnsafe(implicit spark: SparkSession): DataFrame =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  /**
   * Exception-safe build function to return either exception message or `DataFrame`
   */
  def buildSafe(implicit spark: SparkSession): Either[String, DataFrame] = handleException(buildUnsafe)

}
