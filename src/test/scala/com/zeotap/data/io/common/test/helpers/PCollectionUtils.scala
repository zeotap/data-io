package com.zeotap.data.io.common.test.helpers

import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.data.io.helpers.beam.BeamHelpers
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.values.PCollection
import org.apache.spark.sql.DataFrame

object PCollectionUtils {

  def assertPCollectionEqualsDataFrame(expectedDf: DataFrame, actualPCollection: PCollection[GenericRecord], schema: String, sortColumn: String, tempPath: String): Unit = {
    actualPCollection.apply(AvroIO.writeGenericRecords(BeamHelpers.parseAvroSchema(schema)).to(tempPath + "/part").withSuffix(".avro"))
    actualPCollection.getPipeline.run()
    val actualDf = expectedDf.sqlContext.sparkSession.read.format("avro").load(tempPath)

    assertDataFrameEquality(expectedDf, actualDf, sortColumn)
  }

}
