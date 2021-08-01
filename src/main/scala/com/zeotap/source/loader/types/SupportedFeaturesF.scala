package com.zeotap.source.loader.types

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.source.loader.types.SupportedFeatures._

object SupportedFeaturesF {

  type DataSourceLoader[A] = Free[SupportedFeatures, A]

  def addFormat[A](datatype: DataFormatType): DataSourceLoader[A] = liftF(FormatType[A](datatype))

  def basePath[A](path: String): DataSourceLoader[A] = liftF(BasePath[A](path))

  def inferSchema[A]: DataSourceLoader[A] = liftF(InferSchema[A]())

  def header[A]: DataSourceLoader[A] = liftF(Header[A]())

  def separator[A](separator: String): DataSourceLoader[A] = liftF(Separator[A](separator))

  def multiLine[A]: DataSourceLoader[A] = liftF(MultiLine[A]())

  def avroSchema[A](schema: String): DataSourceLoader[A] = liftF(AvroSchema[A](schema))

  def mergeSchema[A]: DataSourceLoader[A] = liftF(MergeSchema[A]())

  def connectionProperties[A](url: String, user: String, password: String, tableName: String): DataSourceLoader[A] = liftF(ConnectionProperties[A](url, user, password, tableName))

  def customSchema[A](schema: String): DataSourceLoader[A] = liftF(CustomSchema[A](schema))

  def load[A](): DataSourceLoader[A] = liftF(Load[A]())

  def loadPath[A](path: String): DataSourceLoader[A] = liftF(LoadPath[A](path))

  def lookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): DataSourceLoader[A] = liftF(LookBack[A](pathTemplate, parameters, lookBackWindow))

  def latestPath[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): DataSourceLoader[A] = liftF(LatestPath[A](pathTemplate, parameters, relativeToCurrentDate))

  def optionalColumns[A](columns: List[OptionalColumn]): DataSourceLoader[A] = liftF(OptionalColumns[A](columns))

  def addCreationTimestamp[A](inputType: String): DataSourceLoader[A] = liftF(AddCreationTimestamp[A](inputType))

}
