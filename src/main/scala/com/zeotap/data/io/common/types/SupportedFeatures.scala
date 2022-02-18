package com.zeotap.data.io.common.types

sealed trait SupportedFeatures[A]

object SupportedFeatures {

  // Reader and Writer-specific features
  final case class FormatType[A](format: DataFormatType) extends SupportedFeatures[A]

  // Reader-specific features
  final case class Schema[A](schema: String) extends SupportedFeatures[A]

  // FS Reader-specific features
  final case class BasePath[A](path: String) extends SupportedFeatures[A]

  // CSV and Text Reader and Writer-specific features
  final case class InferSchema[A]() extends SupportedFeatures[A]

  final case class Header[A]() extends SupportedFeatures[A]

  final case class Separator[A](value: String) extends SupportedFeatures[A]

  // JSON Reader-specific features
  final case class MultiLine[A]() extends SupportedFeatures[A]

  // Avro Reader-specific features
  final case class AvroSchema[A](schema: String) extends SupportedFeatures[A]

  // Parquet Reader-specific features
  final case class MergeSchema[A]() extends SupportedFeatures[A]

  // JDBC Reader and writer-specific features
  final case class ConnectionProperties[A](url: String, user: String, password: String) extends SupportedFeatures[A]

  final case class Driver[A](driver: String) extends SupportedFeatures[A]

  final case class TableName[A](tableName: String) extends SupportedFeatures[A]

  final case class StringType[A](stringType: String) extends SupportedFeatures[A]

  // JDBC Reader specific features
  final case class Query[A](query: String) extends SupportedFeatures[A]

  final case class CustomSchema[A](schema: String) extends SupportedFeatures[A]

  // Loader-specific features
  final case class Load[A]() extends SupportedFeatures[A]

  final case class LoadPath[A](path: String) extends SupportedFeatures[A]

  final case class LoadPaths[A](paths: List[String]) extends SupportedFeatures[A]

  final case class LookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer) extends SupportedFeatures[A]

  final case class LatestPaths[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean) extends SupportedFeatures[A]

  // DataFrame-specific features
  final case class AddOptionalColumns[A](columns: List[OptionalColumn]) extends SupportedFeatures[A]

  // FS DataFrame-specific features
  final case class AddCreationTimestamp[A](operation: String, inputColumn: Option[String], outputColumn: String) extends SupportedFeatures[A]

  // Writer-specific features
  final case class AddSaveMode[A](saveMode: SaveMode) extends SupportedFeatures[A]

  final case class PartitionBy[A](columnNames: List[String]) extends SupportedFeatures[A]

  final case class SaveToPath[A](path: String) extends SupportedFeatures[A]

  final case class Save[A]() extends SupportedFeatures[A]

  // JSON Writer-specific features
  final case class Compression[A](compression: String) extends SupportedFeatures[A]

}
