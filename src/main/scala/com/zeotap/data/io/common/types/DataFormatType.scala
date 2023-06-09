package com.zeotap.data.io.common.types

sealed trait DataFormatType

case object Text extends DataFormatType

case object CSV extends DataFormatType

case object JSON extends DataFormatType

case object Avro extends DataFormatType

case object Parquet extends DataFormatType

case object ORC extends DataFormatType

case object JDBC extends DataFormatType

case object BigQuery extends DataFormatType

case object Delta extends DataFormatType

object DataFormatType {

  def value(format: DataFormatType): String = {
    format match {
      case Text => "text"
      case CSV => "csv"
      case JSON => "json"
      case Avro => "avro"
      case Parquet => "parquet"
      case ORC => "orc"
      case JDBC => "jdbc"
      case BigQuery => "bigquery"
      case Delta => "delta"
    }
  }

  def valueOf(dataFormat: String): DataFormatType = {
    dataFormat match {
      case "text" => Text
      case "csv" => CSV
      case "json" => JSON
      case "avro" => Avro
      case "parquet" => Parquet
      case "orc" => ORC
      case "jdbc" => JDBC
      case "bigquery" => BigQuery
      case "delta" => Delta
    }

}
