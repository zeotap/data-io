package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{BigQuery, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class BigQuerySparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(BigQuery)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends SparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   */
  def load(path: String) =
    BigQuerySparkLoader(readerProperties, readerToDataFrameProperties :+ SupportedFeaturesHelper.loadPath(path), dataFrameProperties)

}
