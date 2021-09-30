package com.zeotap.data.io.sink.utils

import com.zeotap.data.io.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.sink.spark.interpreters.SparkInterpreters.{SparkSinkWriter, writerInterpreter, writerToSinkInterpreter}
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

object SparkWriterUtils {

  def buildWriter(writerProperties: Seq[SupportedFeaturesF[DataFrameWriter[_]]],
                  writerToSinkProperties: Seq[SupportedFeaturesF[Unit]])(implicit dataFrame: DataFrame): Unit = {
    val writer = featuresCompiler(writerProperties).foldMap[SparkSinkWriter](writerInterpreter).run(dataFrame.write)
    featuresCompiler(writerToSinkProperties).foldMap[SparkSinkWriter](writerToSinkInterpreter).run(writer)
  }

}
