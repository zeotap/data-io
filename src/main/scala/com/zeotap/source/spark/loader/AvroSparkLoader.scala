package com.zeotap.source.spark.loader

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.zeotap.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.common.types.{AVRO, SupportedFeaturesHelper}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class AvroSparkLoader(
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(AVRO)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Optional schema can be provided in a JSON string format
   */
  def avroSchema(schema: String): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesHelper.avroSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Optional schema can be provided in a JSON format
   */
  def avroSchema(jsonSchema: JsonNode): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesHelper.avroSchema(new ObjectMapper().writeValueAsString(jsonSchema)), readerToDataFrameProperties, dataFrameProperties)
}
