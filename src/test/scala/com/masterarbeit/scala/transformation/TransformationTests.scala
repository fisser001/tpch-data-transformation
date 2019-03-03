package com.masterarbeit.scala.transformation

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec
import com.masterarbeit.scala.SparkSessionTestWrapper

class TransformationTests extends FunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  import spark.implicits._
  it("transforms api source to prepared") {
    val actualDf = spark.emptyDataFrame
    val expectedDf = spark.emptyDataFrame
    assertSmallDataFrameEquality(actualDf, expectedDf, orderedComparison = false)
  }
}
