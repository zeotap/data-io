package com.zeotap.data.io.source.beam.constructs

import com.zeotap.data.io.common.types.OptionalColumn
import com.zeotap.data.io.helpers.beam.BeamHelpers
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.io.{AvroIO, TextIO}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection

import scala.collection.JavaConverters._

object PCollectionReaderOps {

  implicit class PCollectionReaderExt(pCollectionReader: PCollectionReader) {

    def option(key: String, value: Any): PCollectionReader =
      PCollectionReader(pCollectionReader.options ++ Map(key -> value))

    def options(options: Map[String, Any]): PCollectionReader =
      PCollectionReader(pCollectionReader.options ++ options)

    def load(path: String)(implicit beam: Pipeline): PCollection[GenericRecord] = {
      val (options, schema, schemaWithOptionalColumns) = getOptionsAndSchema
      options("format") match {
        case "avro" => readAvro(schemaWithOptionalColumns, path + "/*.avro")
        case "parquet" => readParquet(schema, schemaWithOptionalColumns, path + "/*.parquet")
        case "text" => readTextOrCsv(schemaWithOptionalColumns, path + "/*.txt", options.getOrElse("delimiter", ",").toString)
        case "csv" => readTextOrCsv(schemaWithOptionalColumns, path + "/*.csv", options.getOrElse("delimiter", ",").toString)
        case "json" => readJSON(schemaWithOptionalColumns, path + "/*.json")
        case _ => throw new IllegalArgumentException("Format not supported!")
      }
    }

    def load()(implicit beam: Pipeline): PCollection[GenericRecord] = {
      val (options, schema, schemaWithOptionalColumns) = getOptionsAndSchema
      options("format") match {
        case "jdbc" => readJDBC(schemaWithOptionalColumns, options("driver").toString, options("url").toString, options("user").toString, options("password").toString, options("query").toString)
        case _ => throw new IllegalArgumentException("Format not supported!")
      }
    }

    private def getOptionsAndSchema: (Map[String, Any], String, String) = {
      val options = pCollectionReader.options
      val schema = options("schema").toString
      val schemaWithOptionalColumns = BeamHelpers.addOptionalColumnsToSchema(schema, options.getOrElse("optionalColumns", List()).asInstanceOf[List[OptionalColumn]].asJava)
      (options, schema, schemaWithOptionalColumns)
    }

  }

  def readAvro(schema: String, path: String)(implicit beam: Pipeline): PCollection[GenericRecord] =
    beam.apply(AvroIO.readGenericRecords(schema).from(path))

  def readParquet(schema: String, schemaWithOptionalColumns: String, path: String)(implicit beam: Pipeline): PCollection[GenericRecord] =
    beam.apply(ParquetIO.read(new Schema.Parser().parse(schema)).from(path))
      .apply(BeamHelpers.addOptionalColumnsToGenericRecord(schema, schemaWithOptionalColumns))
      .setCoder(AvroCoder.of(BeamHelpers.parseAvroSchema(schemaWithOptionalColumns)))

  def readTextOrCsv(schema: String, path: String, delimiter: String)(implicit beam: Pipeline): PCollection[GenericRecord] =
    readMultiFormatText(schema, path, BeamHelpers.convertStringRowToGenericRecord(delimiter, schema))

  def readJSON(schema: String, path: String)(implicit beam: Pipeline): PCollection[GenericRecord] =
    readMultiFormatText(schema, path, BeamHelpers.convertJsonStringRowToGenericRecord(schema))

  def readJDBC(schema: String, driver: String, url: String, user: String, password: String, query: String)(implicit beam: Pipeline): PCollection[GenericRecord] =
    beam.apply(BeamHelpers.readRowAsGenericRecordFromJDBC(schema, driver, url, user, password, query))

  private def readMultiFormatText(schema: String, path: String, transformation: ParDo.SingleOutput[String, GenericRecord])(implicit beam: Pipeline): PCollection[GenericRecord] =
    beam.apply(TextIO.read().from(path)).apply(transformation).setCoder(AvroCoder.of(BeamHelpers.parseAvroSchema(schema)))

}
