package com.zeotap.data.io.common.constructs

import org.apache.spark.sql.Dataset
import org.apache.spark.util.sketch.BloomFilter

object BloomOps {

  def reduceToSingleBloom(bloomDataset: Dataset[BloomFilter]): BloomFilter = {
    bloomDataset.rdd.treeReduce((a, b) => {
      a.mergeInPlace(b)
      a
    })
  }

}
