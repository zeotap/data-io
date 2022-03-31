package com.zeotap.data.io.common.utils

import org.scalatest.FunSuite

class CloudStorePathMetaGeneratorTest extends FunSuite {

  test("RawTS When Input Path ends with *") {
    val inputPathWithAsterisk: String = "src/test/resources/custom-input-format/yr=2022/mon=03/*"
    val inputPathList1 = Array(inputPathWithAsterisk)

    val inputPathWithoutAsterisk: String = "src/test/resources/custom-input-format/yr=2022/mon=03"
    val inputPathList2 = Array(inputPathWithoutAsterisk)

    val cloudStorePathMetaGenerator = new CloudStorePathMetaGenerator

    val rawTsMap1 = cloudStorePathMetaGenerator.partFileRawTsMapGenerator(inputPathList1)
    val rawTsMap2 = cloudStorePathMetaGenerator.partFileRawTsMapGenerator(inputPathList2)

    assert(rawTsMap1 == rawTsMap2 && rawTsMap1.nonEmpty)
  }
}
