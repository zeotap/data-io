package com.zeotap.data.io.sink.beam.constructs

import com.zeotap.data.io.helpers.beam.BeamHelpers
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.{AvroIO, TextIO}
import org.apache.beam.sdk.values.PCollection

object PCollectionWriterOps {

  implicit class PCollectionReaderExt(pCollectionWriter: PCollectionWriter) {

    def option(key: String, value: Any): PCollectionWriter =
      PCollectionWriter(pCollectionWriter.pCollection, pCollectionWriter.options ++ Map(key -> value))

    def options(options: Map[String, Any]): PCollectionWriter =
      PCollectionWriter(pCollectionWriter.pCollection, pCollectionWriter.options ++ options)

    def save(path: String): Unit = {
      val (options, schema) = getOptionsAndSchema
      implicit val pCollection: PCollection[GenericRecord] = pCollectionWriter.pCollection
        .apply(BeamHelpers.convertRowToGenericRecord()).setCoder(AvroCoder.of(BeamHelpers.parseAvroSchema(schema)))
      options("format") match {
        case "avro" => writeAvro(schema, path + "/part")
        case "parquet" => writeParquet(schema, path)
        case "text" => writeTextOrCsv(schema, path + "/part", options.getOrElse("delimiter", ",").toString, "txt")
        case "csv" => writeTextOrCsv(schema, path + "/part", options.getOrElse("delimiter", ",").toString, "csv")
        case "json" => writeJSON(schema, path + "/part")
        case _ => throw new IllegalArgumentException("Format not supported!")
      }
    }

    def save(): Unit = {
      val (options, schema) = getOptionsAndSchema
      implicit val pCollection: PCollection[GenericRecord] = pCollectionWriter.pCollection
        .apply(BeamHelpers.convertRowToGenericRecord()).setCoder(AvroCoder.of(BeamHelpers.parseAvroSchema(schema)))
      options("format") match {
        case "jdbc" => writeJDBC(schema, options("driver").toString, options("url").toString, options("user").toString, options("password").toString, options("tableName").toString)
        case _ => throw new IllegalArgumentException("Format not supported!")
      }
    }

    private def getOptionsAndSchema: (Map[String, Any], String) = {
      val options = pCollectionWriter.options
      val schema = options("schema").toString
      (options, schema)
    }

  }

  def writeAvro(schema: String, path: String)(implicit pCollection: PCollection[GenericRecord]): Unit =
    pCollection.apply(AvroIO.writeGenericRecords(BeamHelpers.parseAvroSchema(schema)).to(path).withSuffix(".avro"))

  def writeParquet(schema: String, path: String)(implicit pCollection: PCollection[GenericRecord]): Unit =
    pCollection.apply(BeamHelpers.writeGenericRecordToParquet(schema).to(path).withSuffix(".parquet"))

  def writeTextOrCsv(schema: String, path: String, delimiter: String, format: Any)(implicit pCollection: PCollection[GenericRecord]): Unit =
    pCollection.apply(BeamHelpers.convertGenericRecordToDelimitedString(schema, delimiter))
      .apply(TextIO.write().to(path).withSuffix(f".$format"))

  def writeJSON(schema: String, path: String)(implicit pCollection: PCollection[GenericRecord]): Unit =
    pCollection.apply(BeamHelpers.convertGenericRecordToJsonString(schema))
      .apply(TextIO.write().to(path).withSuffix(".json"))

  def writeJDBC(schema: String, driver: String, url: String, user: String, password: String, tableName: String)(implicit pCollection: PCollection[GenericRecord]): Unit =
    pCollection.apply(BeamHelpers.writeGenericRecordToJDBC(schema, driver, url, user, password, tableName))
}
