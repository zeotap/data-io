package com.zeotap.source.loader.spark.loaders

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{AVRO, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class AvroSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(AVRO)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * Optional schema can be provided in a JSON string format
   */
  def avroSchema(schema: String): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesF.avroSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  /**
   * Optional schema can be provided in a JSON format
   */
  def avroSchema(jsonSchema: JsonNode): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesF.avroSchema(new ObjectMapper().writeValueAsString(jsonSchema)), readerToDataFrameProperties, dataFrameProperties)
}