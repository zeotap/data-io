package com.zeotap.data.io.source.spark.loader

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.common.types.{Avro, SupportedFeaturesHelper}
import org.apache.spark.sql.DataFrameReader

case class AvroSparkLoader[A](
  readerProperties: Seq[SupportedFeaturesF[DataFrameReader]] = Seq(SupportedFeaturesHelper.addFormat(Avro)),
  readerToDataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq(),
  dataFrameProperties: Seq[SupportedFeaturesF[A]] = Seq()
) extends FSSparkLoader[A](readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Optional schema can be provided in a JSON string format
   */
  def avroSchema(schema: String) =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesHelper.avroSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Optional schema can be provided in a JSON format
   */
  def avroSchema(jsonSchema: JsonNode) =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesHelper.avroSchema(new ObjectMapper().writeValueAsString(jsonSchema)), readerToDataFrameProperties, dataFrameProperties)
}
