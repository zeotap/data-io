package com.zeotap.source.loader.spark.interpreters

import cats.arrow.FunctionK
import cats.data.{Reader, State}
import com.zeotap.source.loader.spark.constructs.DataFrameOps._
import com.zeotap.source.loader.spark.constructs.DataFrameReaderOps._
import com.zeotap.source.loader.types.SupportedFeatures._
import com.zeotap.source.loader.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader}

object SparkInterpreters {

  type SparkReader[A] = Reader[DataFrameReader, A]

  type SparkDataFrame[A] = State[DataFrame, A]

  val readerInterpreter: FunctionK[SupportedFeatures, SparkReader] = new FunctionK[SupportedFeatures, SparkReader] {
    override def apply[A](feature: SupportedFeatures[A]): SparkReader[A] = Reader { dataFrameReader =>
      val reader: DataFrameReader = feature match {
        case FormatType(format) => dataFrameReader.format(DataFormatType.value(format))
        case BasePath(path) => dataFrameReader.option("basePath", path)
        case Separator(separator) => dataFrameReader.option("sep", separator)
        case InferSchema() => dataFrameReader.option("inferSchema", "true")
        case Header() => dataFrameReader.option("header", "true")
        case MultiLine() => dataFrameReader.option("multiLine", "true")
        case AvroSchema(jsonSchema) => dataFrameReader.option("avroSchema", jsonSchema)
        case MergeSchema() => dataFrameReader.option("mergeSchema", "true")
        case ConnectionProperties(url, user, password, tableName) => dataFrameReader.option("url", url).option("user", user).option("password", password).option("dbtable", tableName)
        case CustomSchema(schema) => dataFrameReader.option("customSchema", schema)
        case _ => dataFrameReader
      }
      reader.asInstanceOf[A]
    }
  }

  val readerToDataFrameInterpreter: FunctionK[SupportedFeatures, SparkReader] = new FunctionK[SupportedFeatures, SparkReader] {
    override def apply[A](feature: SupportedFeatures[A]): SparkReader[A] = Reader { dataFrameReader =>
      val dataFrame: DataFrame = feature match {
        case LoadPath(path) => dataFrameReader.load(path)
        case Load() => dataFrameReader.load()
        case LookBack(pathTemplate, parameters, lookBackWindow) => dataFrameReader.lookBack(pathTemplate, parameters, lookBackWindow)
        case LatestPath(pathTemplate, parameters, relativeToCurrentDate) => dataFrameReader.latestPath(pathTemplate, parameters, relativeToCurrentDate)
        case _ => dataFrameReader.load()
      }
      dataFrame.asInstanceOf[A]
    }
  }

  val dataFrameInterpreter: FunctionK[SupportedFeatures, SparkDataFrame] = new FunctionK[SupportedFeatures, SparkDataFrame] {
    override def apply[A](feature: SupportedFeatures[A]): SparkDataFrame[A] = State { sparkDataFrame =>
      val dataFrame: DataFrame = feature match {
        case OptionalColumns(columns) => sparkDataFrame.optionalColumns(columns)
        case AddCreationTimestamp(inputType) => sparkDataFrame.appendRawTsToDataFrame(inputType)
        case _ => sparkDataFrame
      }
      (dataFrame, sparkDataFrame.asInstanceOf[A])
    }
  }
}
