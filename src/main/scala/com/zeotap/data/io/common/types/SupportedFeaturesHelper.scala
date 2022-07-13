package com.zeotap.data.io.common.types

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.data.io.common.types.SupportedFeatures._

object SupportedFeaturesHelper {

  type SupportedFeaturesF[A] = Free[SupportedFeatures, A]

  def addFormat[A](datatype: DataFormatType): SupportedFeaturesF[A] = liftF(FormatType[A](datatype))

  def schema[A](schema: String): SupportedFeaturesF[A] = liftF(Schema[A](schema))

  def basePath[A](path: String): SupportedFeaturesF[A] = liftF(BasePath[A](path))

  def inferSchema[A]: SupportedFeaturesF[A] = liftF(InferSchema[A]())

  def header[A]: SupportedFeaturesF[A] = liftF(Header[A]())

  def separator[A](separator: String): SupportedFeaturesF[A] = liftF(Separator[A](separator))

  def multiLine[A]: SupportedFeaturesF[A] = liftF(MultiLine[A]())

  def avroSchema[A](schema: String): SupportedFeaturesF[A] = liftF(AvroSchema[A](schema))

  def mergeSchema[A]: SupportedFeaturesF[A] = liftF(MergeSchema[A]())

  def connectionProperties[A](url: String, user: String, password: String): SupportedFeaturesF[A] = liftF(ConnectionProperties[A](url, user, password))

  def driver[A](driver: String): SupportedFeaturesF[A] = liftF(Driver[A](driver))

  def tableName[A](tableName: String): SupportedFeaturesF[A] = liftF(TableName[A](tableName))

  def stringType[A](stringType: String): SupportedFeaturesF[A] = liftF(StringType[A](stringType))

  def query[A](query: String): SupportedFeaturesF[A] = liftF(Query[A](query))

  def customSchema[A](schema: String): SupportedFeaturesF[A] = liftF(CustomSchema[A](schema))

  def load[A](): SupportedFeaturesF[A] = liftF(Load[A]())

  def loadPath[A](path: String): SupportedFeaturesF[A] = liftF(LoadPath[A](path))

  def loadPaths[A](paths: List[String]): SupportedFeaturesF[A] = liftF(LoadPaths[A](paths))

  def distributedLoad[A](numberOfPartitions:Option[Int],intermediatePath:String,prioritiseIntermediatePath:Option[Boolean]):SupportedFeaturesF[A] = liftF(DistributedLoad(numberOfPartitions,intermediatePath,prioritiseIntermediatePath))

  def lookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): SupportedFeaturesF[A] = liftF(LookBack[A](pathTemplate, parameters, lookBackWindow))

  def latestPaths[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): SupportedFeaturesF[A] = liftF(LatestPaths[A](pathTemplate, parameters, relativeToCurrentDate))

  def addOptionalColumns[A](columns: List[OptionalColumn]): SupportedFeaturesF[A] = liftF(AddOptionalColumns[A](columns))

  def addCreationTimestamp[A](operation: String, inputColumn: Option[String], outputColumn: String): SupportedFeaturesF[A] = liftF(AddCreationTimestamp[A](operation, inputColumn, outputColumn))

  def addSaveMode[A](saveMode: SaveMode): SupportedFeaturesF[A] = liftF(AddSaveMode[A](saveMode))

  def partitionBy[A](columnNames: List[String]): SupportedFeaturesF[A] = liftF(PartitionBy[A](columnNames))

  def saveToPath[A](path: String): SupportedFeaturesF[A] = liftF(SaveToPath[A](path))

  def save[A](): SupportedFeaturesF[A] = liftF(Save[A]())

  def compression[A](compression: String): SupportedFeaturesF[A] = liftF(Compression[A](compression))

}
