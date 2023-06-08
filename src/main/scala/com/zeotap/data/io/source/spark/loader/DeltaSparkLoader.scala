package com.zeotap.data.io.source.spark.loader

import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Delta, SupportedFeaturesHelper}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class DeltaSparkLoader(
    readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(Delta)),
    readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
    dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

}
