package com.zeotap.data.io.sink.beam.constructs

import org.apache.beam.sdk.values.{PCollection, Row}

case class PCollectionWriter(pCollection: PCollection[Row], options: Map[String, Any])
