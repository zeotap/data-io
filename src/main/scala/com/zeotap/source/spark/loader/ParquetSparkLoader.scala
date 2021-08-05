package com.zeotap.source.spark.loader

import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF
import com.zeotap.common.types.{PARQUET, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class ParquetSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(PARQUET)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the sub-directories in a given path contain datasets with different schemas,
   * this option will read the data into a single DataFrame with all the columns
   */
  def mergeSchema: ParquetSparkLoader =
    ParquetSparkLoader(readerProperties :+ SupportedFeaturesF.mergeSchema, readerToDataFrameProperties, dataFrameProperties)
}
