package com.zeotap.data.io.sink.spark.constructs

import com.zeotap.data.io.common.constructs.BloomOps.reduceToSingleBloom
import com.zeotap.data.io.sink.utils.BloomFilterUtils.createIteratorofColBloom
import org.apache.spark.sql._
import org.apache.spark.util.sketch.BloomFilter

object DataFrameOps {

  implicit class DataFrameExt(dataFrame: DataFrame) {

    def writeBloomFilter(columns: List[String], bloomBasePath: String, expectedInsertions: Long, fpp: Double)(implicit spark: SparkSession) {

      val encoder: Encoder[BloomFilter] = Encoders.kryo(classOf[BloomFilter])
      columns.foreach(colName => {
        val value = dataFrame.rdd.mapPartitions { iter => createIteratorofColBloom(iter, colName, expectedInsertions, fpp) }
        val intermediateDf = spark.createDataset[BloomFilter](value)(encoder)

        dataFrame.writeBloomFilter(intermediateDf, reduceToSingleBloom, s"$bloomBasePath/$colName")
      })
    }

    def writeBloomFilter(bloomDataset: Dataset[BloomFilter], action: Dataset[BloomFilter] => BloomFilter, bloomPath: String)(implicit spark: SparkSession) {
      val encoder: Encoder[BloomFilter] = Encoders.kryo(classOf[BloomFilter])

      val bloom: BloomFilter = action(bloomDataset)
      val bloomdf = spark.createDataset[BloomFilter](List(bloom))(encoder)

      bloomdf.write.parquet(bloomPath)
    }
  }

}
