package com.zeotap.sink.spark.interpreters

import cats.arrow.FunctionK
import cats.data.Reader
import com.zeotap.common.types.{DataFormatType, SaveMode, SupportedFeatures}
import com.zeotap.common.types.SupportedFeatures._
import org.apache.spark.sql.DataFrameWriter

object SparkInterpreters {

  type SparkSinkWriter[A] = Reader[DataFrameWriter[_], A]

  val writerInterpreter: FunctionK[SupportedFeatures, SparkSinkWriter] = new FunctionK[SupportedFeatures, SparkSinkWriter] {
    override def apply[A](feature: SupportedFeatures[A]): SparkSinkWriter[A] = Reader { dataFrameWriter =>
      val writer: DataFrameWriter[_] = feature match {
        case FormatType(format) => dataFrameWriter.format(DataFormatType.value(format))
        case AddSaveMode(saveMode) => dataFrameWriter.mode(SaveMode.value(saveMode))
        case PartitionBy(columnNames) => dataFrameWriter.partitionBy(columnNames : _*)
        case ConnectionProperties(url, user, password) => dataFrameWriter.option("url", url).option("user", user).option("password", password)
        case TableName(tableName) => dataFrameWriter.option("dbtable", tableName)
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
