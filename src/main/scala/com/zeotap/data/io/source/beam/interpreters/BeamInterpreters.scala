package com.zeotap.data.io.source.beam.interpreters

import cats.arrow.FunctionK
import cats.data.{Reader, State}
import com.zeotap.data.io.common.types.SupportedFeatures._
import com.zeotap.data.io.common.types.{DataFormatType, SupportedFeatures}
import com.zeotap.data.io.source.beam.constructs.PCollectionReader
import com.zeotap.data.io.source.beam.constructs.PCollectionReaderOps.PCollectionReaderExt
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, Row}

object BeamInterpreters {

  type BeamReaderState[A] = State[PCollectionReader, A]

  type BeamReader[A] = Reader[PCollectionReader, A]

  val readerInterpreter: FunctionK[SupportedFeatures, BeamReaderState] = new FunctionK[SupportedFeatures, BeamReaderState] {
    override def apply[A](feature: SupportedFeatures[A]): BeamReaderState[A] = State { pCollectionReader =>
      val reader: PCollectionReader = feature match {
        case FormatType(format) => pCollectionReader.option("format", DataFormatType.value(format))
        case Separator(separator) => pCollectionReader.option("delimiter", separator)
        case Schema(schema) => pCollectionReader.option("schema", schema)
        case AddOptionalColumns(optionalColumns) => pCollectionReader.option("optionalColumns", optionalColumns)
        case ConnectionProperties(url, user, password) => pCollectionReader.option("url", url).option("user", user).option("password", password)
        case Driver(driver) => pCollectionReader.option("driver", driver)
        case TableName(tableName) => pCollectionReader.option("query", f"select * from $tableName")
        case Query(query) => pCollectionReader.option("query", query)
        case _ => pCollectionReader
      }
      (reader, pCollectionReader.asInstanceOf[A])
    }
  }

  def readerToPCollectionInterpreter(implicit beam: Pipeline): FunctionK[SupportedFeatures, BeamReader] = new FunctionK[SupportedFeatures, BeamReader] {
    override def apply[A](feature: SupportedFeatures[A]): BeamReader[A] = Reader { pCollectionReader =>
      val pCollection: PCollection[Row] = feature match {
        case Load() => pCollectionReader.load()
        case LoadPath(path) => pCollectionReader.load(path)
      }
      pCollection.asInstanceOf[A]
    }
  }

}
