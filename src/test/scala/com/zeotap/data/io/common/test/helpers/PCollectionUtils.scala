package com.zeotap.data.io.common.test.helpers

import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.data.io.helpers.beam.BeamHelpers
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.values.{PCollection, Row}
import org.apache.spark.sql.DataFrame

object PCollectionUtils {

  def assertPCollectionEqualsDataFrame(expectedDf: DataFrame, actualPCollection: PCollection[Row], schema: String, sortColumn: String, tempPath: String): Unit = {
    actualPCollection
      .apply(BeamHelpers.convertRowToGenericRecord()).setCoder(AvroCoder.of(BeamHelpers.parseAvroSchema(schema)))
      .apply(AvroIO.writeGenericRecords(BeamHelpers.parseAvroSchema(schema)).to(tempPath + "/part").withSuffix(".avro"))
    actualPCollection.getPipeline.run()
    val actualDf = expectedDf.sqlContext.sparkSession.read.format("avro").load(tempPath)

    assertDataFrameEquality(expectedDf, actualDf, sortColumn)
  }

}
