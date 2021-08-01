package com.zeotap.source.loader.types

sealed trait SupportedFeatures[A]

object SupportedFeatures {

  // FS Reader-specific features
  final case class FormatType[A](format: DataFormatType) extends SupportedFeatures[A]

  final case class BasePath[A](path: String) extends SupportedFeatures[A]

  // CSV Reader-specific features
  final case class InferSchema[A]() extends SupportedFeatures[A]

  final case class Header[A]() extends SupportedFeatures[A]

  final case class Separator[A](value: String) extends SupportedFeatures[A]

  // JSON Reader-specific features
  final case class MultiLine[A]() extends SupportedFeatures[A]

  // Avro Reader-specific features
  final case class AvroSchema[A](schema: String) extends SupportedFeatures[A]

  // Parquet Reader-specific features
  final case class MergeSchema[A]() extends SupportedFeatures[A]

  // JDBC Reader-specific features
  final case class ConnectionProperties[A](url: String, user: String, password: String, tableName: String) extends SupportedFeatures[A]

  final case class CustomSchema[A](schema: String) extends SupportedFeatures[A]

  // Loader-specific features
  final case class Load[A]() extends SupportedFeatures[A]

  final case class LoadPath[A](path: String) extends SupportedFeatures[A]

  final case class LookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer) extends SupportedFeatures[A]

  final case class LatestPath[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean) extends SupportedFeatures[A]

  // DataFrame-specific features
  final case class OptionalColumns[A](columns: List[OptionalColumn]) extends SupportedFeatures[A]

  // FS DataFrame-specific features
  final case class AddCreationTimestamp[A](inputType: String) extends SupportedFeatures[A]

}
