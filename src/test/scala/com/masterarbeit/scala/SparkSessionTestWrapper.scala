package com.masterarbeit.scala

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    var sparkSession: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("tpc-data-transformation-spark-tests")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession
  }
}
