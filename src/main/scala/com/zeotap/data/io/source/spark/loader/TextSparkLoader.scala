package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{SupportedFeaturesHelper, Text}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class TextSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(Text)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Custom separator/delimiter can be provided for the Text file
   */
  def separator(separator: String): TextSparkLoader =
    TextSparkLoader(readerProperties :+ SupportedFeaturesHelper.separator(separator), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Schema for each column is inferred by Spark internally for the Text dataset
   */
  def inferSchema: TextSparkLoader =
    TextSparkLoader(readerProperties :+ SupportedFeaturesHelper.inferSchema, readerToDataFrameProperties, dataFrameProperties)

  /**
   * If the first row of the Text dataset denotes the headers for the columns,
   * this option can be provided to read the Text dataset with column names
   */
  def header: TextSparkLoader =
    TextSparkLoader(readerProperties :+ SupportedFeaturesHelper.header, readerToDataFrameProperties, dataFrameProperties)

}
