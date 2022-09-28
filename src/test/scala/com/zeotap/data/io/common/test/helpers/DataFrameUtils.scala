package com.zeotap.data.io.common.test.helpers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite

object DataFrameUtils extends FunSuite with DataFrameSuiteBase {

  def assertDataFrameEquality(expectedDf: DataFrame, actualDf: DataFrame, sortColumn: String): Unit = {
    val expectedColumns = expectedDf.columns.sorted.map(col)
    val actualColumns = actualDf.columns.sorted.map(col)

    assertDataFrameEquals(setNullableStateForAllColumns(expectedDf, true).select(expectedColumns : _*).distinct.orderBy(sortColumn),
      setNullableStateForAllColumns(actualDf, true).select(actualColumns : _*).distinct.orderBy(sortColumn))
  }

  def unionByName(df1: DataFrame, df2: DataFrame): DataFrame = {
    val df1Columns = df1.columns.sorted.map(col)
    val df2Columns = df2.columns.sorted.map(col)

    df1.select(df1Columns : _*).union(df2.select(df2Columns : _*))
  }

  private def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean) : DataFrame = {
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
  }

}
