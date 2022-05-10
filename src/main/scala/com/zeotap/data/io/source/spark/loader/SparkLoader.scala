package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{OptionalColumn, SourceLoader, SupportedFeaturesHelper}
import com.zeotap.data.io.common.utils.CommonUtils.handleException
import com.zeotap.data.io.source.utils.SparkLoaderUtils
import org.apache.spark.sql.{DataFrameReader, SparkSession}

object SparkLoader {

  def text[A]() = TextSparkLoader[A]()

  def csv[A]() = CSVSparkLoader[A]()

  def json[A]() = JSONSparkLoader[A]()

  def avro[A]() = AvroSparkLoader[A]()

  def parquet[A]() = ParquetSparkLoader[A]()

  def orc[A]() = ORCSparkLoader[A]()

  def jdbc[A]() = JDBCSparkLoader[A]()

  def bigquery[A]() = BigQuerySparkLoader[A]()

}

class SparkLoader[A](readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(),
                     readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
                     dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
                    ) extends SourceLoader {

  /**
   * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
   * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
   *
   * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
   *                Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
   */
  def addOptionalColumns(columns: List[OptionalColumn]) =
    new SparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties :+ SupportedFeaturesHelper.addOptionalColumns(columns))

  /**
   * Returns a `DataFrame` based on all the provided reader and dataFrame properties
   */
  def buildUnsafe(implicit spark: SparkSession): A =
    SparkLoaderUtils.buildLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties)

  /**
   * Exception-safe build function to return either exception message or `DataFrame`
   */
  def buildSafe(implicit spark: SparkSession): Either[String, A] = handleException(buildUnsafe)

}
