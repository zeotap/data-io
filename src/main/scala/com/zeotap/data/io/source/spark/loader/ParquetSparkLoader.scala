package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Parquet, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class ParquetSparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(Parquet)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the sub-directories in a given path contain datasets with different schemas,
   * this option will read the data into a single DataFrame with all the columns
   */
  def mergeSchema =
    ParquetSparkLoader(readerProperties :+ SupportedFeaturesHelper.mergeSchema, readerToDataFrameProperties, dataFrameProperties)
}
