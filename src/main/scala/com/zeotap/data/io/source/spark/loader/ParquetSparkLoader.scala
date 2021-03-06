package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Parquet, SupportedFeaturesHelper}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class ParquetSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(Parquet)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the sub-directories in a given path contain datasets with different schemas,
   * this option will read the data into a single DataFrame with all the columns
   */
  def mergeSchema: ParquetSparkLoader =
    ParquetSparkLoader(readerProperties :+ SupportedFeaturesHelper.mergeSchema, readerToDataFrameProperties, dataFrameProperties)
}
