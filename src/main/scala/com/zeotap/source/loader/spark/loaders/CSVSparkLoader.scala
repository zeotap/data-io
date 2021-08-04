package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{CSV, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class CSVSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(CSV)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Custom separator/delimiter can be provided for the CSV file
   */
  def separator(separator: String): CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.separator(separator), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Schema for each column is inferred by Spark internally for the CSV dataset
   */
  def inferSchema: CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.inferSchema, readerToDataFrameProperties, dataFrameProperties)

  /**
   * If the first row of the CSV dataset denotes the headers for the columns,
   * this option can be provided to read the CSV with column names
   */
  def header: CSVSparkLoader =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesF.header, readerToDataFrameProperties, dataFrameProperties)
}
