package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF
import com.zeotap.common.types.{CSV, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class CSVSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(CSV)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
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
