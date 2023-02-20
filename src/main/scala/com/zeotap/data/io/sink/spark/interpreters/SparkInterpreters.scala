package com.zeotap.data.io.sink.spark.interpreters

import cats.arrow.FunctionK
import cats.data.Reader
import com.zeotap.data.io.common.types.{DataFormatType, SaveMode, SupportedFeatures}
import com.zeotap.data.io.common.types.SupportedFeatures._
import org.apache.spark.sql.DataFrameWriter

object SparkInterpreters {

  type SparkSinkWriter[A] = Reader[DataFrameWriter[_], A]

  val writerInterpreter: FunctionK[SupportedFeatures, SparkSinkWriter] = new FunctionK[SupportedFeatures, SparkSinkWriter] {
    override def apply[A](feature: SupportedFeatures[A]): SparkSinkWriter[A] = Reader { dataFrameWriter =>
      val writer: DataFrameWriter[_] = feature match {
        case FormatType(format) => dataFrameWriter.format(DataFormatType.value(format))
        case Separator(separator) => dataFrameWriter.option("sep", separator)
        case InferSchema() => dataFrameWriter.option("inferSchema", "true")
        case Header() => dataFrameWriter.option("header", "true")
        case Compression(compression) => dataFrameWriter.option("compression", compression)
        case AddSaveMode(saveMode) => dataFrameWriter.mode(SaveMode.value(saveMode))
        case PartitionBy(columnNames) => dataFrameWriter.partitionBy(columnNames : _*)
        case ConnectionProperties(url, user, password) => dataFrameWriter.option("url", url).option("user", user).option("password", password)
        case Driver(driver) => dataFrameWriter.option("driver", driver)
        case TableName(tableName) => dataFrameWriter.option("dbtable", tableName)
        case StringType(stringType) => dataFrameWriter.option("stringtype", stringType)
        case BatchSize(batchSize) => dataFrameWriter.option("batchsize", batchSize)
        case _ => dataFrameWriter
      }
      writer.asInstanceOf[A]
    }
  }

  val writerToSinkInterpreter: FunctionK[SupportedFeatures, SparkSinkWriter] = new FunctionK[SupportedFeatures, SparkSinkWriter] {
    override def apply[A](feature: SupportedFeatures[A]): SparkSinkWriter[A] = Reader { dataFrameWriter =>
      val unit: Unit = feature match {
        case SaveToPath(path) => dataFrameWriter.save(path)
        case Save() => dataFrameWriter.save()
        case _ => dataFrameWriter.save()
      }
      unit.asInstanceOf[A]
    }
  }

}
