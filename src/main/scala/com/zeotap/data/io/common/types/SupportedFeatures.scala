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

  final case class BatchSize[A](batchSize: String) extends SupportedFeatures[A]

  final case class NumPartitions[A](numPartitions: String) extends SupportedFeatures[A]

  final case class CustomSchema[A](schema: String) extends SupportedFeatures[A]

  // Loader-specific features
  final case class Load[A]() extends SupportedFeatures[A]

  final case class LoadPath[A](path: String) extends SupportedFeatures[A]

  final case class LoadPaths[A](paths: List[String]) extends SupportedFeatures[A]

  final case class LookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer) extends SupportedFeatures[A]

  final case class LatestPaths[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean) extends SupportedFeatures[A]

  /*
  Problem :
  * Custom handling for Single File Issue - a case where only one large data file is present (say 10gb) and consequently would only load the data on one executor (apache spark) and try all computations on that node essentially losing all advantages of a distributed framework.

  Feature :
  * For Apache Spark, resolution is before loading such kind of data for computation,
  * We first partition the data and write it onto some intermediate path, then load this partitioned data and proceed with the computation.
  * We decide the number of partition by passing an optional parameter numberOfPartitions, the default value is 200
  * We silently always load the data from this intermediate path if it is present (We determine presence using _SUCCESS file)
  * If not present, we always partition the data into this intermediate path and consequently load from there.
  * We can change the behaviour by passing an optional parameter prioritiseIntermediatePath (Boolean) as false which forces the repartition even if some data is present at intermediate path.
  *
  * Imp: Although we are re-partitioning the dataset, the user should tune the spark prop `spark.default.parallelism` for better performance.
  */
  final case class DistributedLoad[A](intermediatePath: String, numberOfPartitions: Int, prioritiseIntermediatePath: Boolean) extends SupportedFeatures[A]

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
