package com.zeotap.common.types

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.common.types.SupportedFeatures._

object SupportedFeaturesHelper {

  type SupportedFeaturesF[A] = Free[SupportedFeatures, A]

  def schema[A](schema: String): SupportedFeaturesF[A] = liftF(Schema[A](schema))

  def addFormat[A](datatype: DataFormatType): SupportedFeaturesF[A] = liftF(FormatType[A](datatype))

  def basePath[A](path: String): SupportedFeaturesF[A] = liftF(BasePath[A](path))

  def inferSchema[A]: SupportedFeaturesF[A] = liftF(InferSchema[A]())

  def header[A]: SupportedFeaturesF[A] = liftF(Header[A]())

  def separator[A](separator: String): SupportedFeaturesF[A] = liftF(Separator[A](separator))

  def multiLine[A]: SupportedFeaturesF[A] = liftF(MultiLine[A]())

  def avroSchema[A](schema: String): SupportedFeaturesF[A] = liftF(AvroSchema[A](schema))

  def mergeSchema[A]: SupportedFeaturesF[A] = liftF(MergeSchema[A]())

  def connectionProperties[A](url: String, user: String, password: String, tableName: String): SupportedFeaturesF[A] = liftF(ConnectionProperties[A](url, user, password, tableName))

  def customSchema[A](schema: String): SupportedFeaturesF[A] = liftF(CustomSchema[A](schema))

  def load[A](): SupportedFeaturesF[A] = liftF(Load[A]())

  def loadPath[A](path: String): SupportedFeaturesF[A] = liftF(LoadPath[A](path))

  def loadPaths[A](paths: List[String]): SupportedFeaturesF[A] = liftF(LoadPaths[A](paths))

  def lookBack[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): SupportedFeaturesF[A] = liftF(LookBack[A](pathTemplate, parameters, lookBackWindow))

  def latestPath[A](pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): SupportedFeaturesF[A] = liftF(LatestPath[A](pathTemplate, parameters, relativeToCurrentDate))

  def addOptionalColumns[A](columns: List[OptionalColumn]): SupportedFeaturesF[A] = liftF(AddOptionalColumns[A](columns))

  def addCreationTimestamp[A](inputType: String): SupportedFeaturesF[A] = liftF(AddCreationTimestamp[A](inputType))

}
