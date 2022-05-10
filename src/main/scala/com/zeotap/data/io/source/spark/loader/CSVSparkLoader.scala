package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{CSV, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class CSVSparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(CSV)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Custom separator/delimiter can be provided for the CSV file
   */
  def separator(separator: String) =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesHelper.separator(separator), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Schema for each column is inferred by Spark internally for the CSV dataset
   */
  def inferSchema =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesHelper.inferSchema, readerToDataFrameProperties, dataFrameProperties)

  /**
   * If the first row of the CSV dataset denotes the headers for the columns,
   * this option can be provided to read the CSV with column names
   */
  def header =
    CSVSparkLoader(readerProperties :+ SupportedFeaturesHelper.header, readerToDataFrameProperties, dataFrameProperties)

}
