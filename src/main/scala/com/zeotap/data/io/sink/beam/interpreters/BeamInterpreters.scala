package com.zeotap.data.io.sink.beam.interpreters

import cats.arrow.FunctionK
import cats.data.{Reader, State}
import com.zeotap.data.io.common.types.SupportedFeatures._
import com.zeotap.data.io.common.types.{DataFormatType, SupportedFeatures}
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriter
import com.zeotap.data.io.sink.beam.constructs.PCollectionWriterOps._

object BeamInterpreters {

  type BeamWriterState[A] = State[PCollectionWriter, A]

  type BeamWriter[A] = Reader[PCollectionWriter, A]

  val writerInterpreter: FunctionK[SupportedFeatures, BeamWriterState] = new FunctionK[SupportedFeatures, BeamWriterState] {
    override def apply[A](feature: SupportedFeatures[A]): BeamWriterState[A] = State { pCollectionWriter =>
      val reader: PCollectionWriter = feature match {
        case FormatType(format) => pCollectionWriter.option("format", DataFormatType.value(format))
        case Separator(separator) => pCollectionWriter.option("delimiter", separator)
        case Schema(schema) => pCollectionWriter.option("schema", schema)
        case ConnectionProperties(url, user, password) => pCollectionWriter.option("url", url).option("user", user).option("password", password)
        case Driver(driver) => pCollectionWriter.option("driver", driver)
        case TableName(tableName) => pCollectionWriter.option("tableName", tableName)
        case _ => pCollectionWriter
      }
      (reader, pCollectionWriter.asInstanceOf[A])
    }
  }

  val writerToSinkInterpreter: FunctionK[SupportedFeatures, BeamWriter] = new FunctionK[SupportedFeatures, BeamWriter] {
    override def apply[A](feature: SupportedFeatures[A]): BeamWriter[A] = Reader { pCollectionWriter =>
      val unit: Unit = feature match {
        case Save() => pCollectionWriter.save()
        case SaveToPath(path) => pCollectionWriter.save(path)
      }
      unit.asInstanceOf[A]
    }
  }

}
