package com.zeotap.data.io.sink.utils

import org.apache.spark.sql.Row
import org.apache.spark.util.sketch.BloomFilter

object BloomFilterUtils {

  def createBloom(expectedInsertions: Long, fpp: Double): BloomFilter = {
    BloomFilter.create(expectedInsertions, fpp)
  }

  def createIteratorofColBloom(iterator: Iterator[Row], colName: String, expectedInsertions: Long, fpp: Double): Iterator[BloomFilter] = {
    val bloom: BloomFilter = createBloom(expectedInsertions, fpp)
    iterator.foreach(row => {
      bloom.put(row.getAs[String](colName))
    })
    Iterator(bloom)
  }
}
