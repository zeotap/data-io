package com.zeotap.data.io.sink.beam.constructs

import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.PCollection

case class PCollectionWriter(pCollection: PCollection[GenericRecord], options: Map[String, Any])
