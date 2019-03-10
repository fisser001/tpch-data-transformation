package com.masterarbeit.scala.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformationJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.crossJoin.enabled", true)
    spark.conf.set("spark.sql.orc.enabled",false)

    val sf = 1

    val customer_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\customer.csv"
    val customer_schema = StructType(Array(
      StructField("c_custkey", StringType, true),
      StructField("c_name", StringType, true),
      StructField("c_address", StringType, true),
      StructField("c_nationkey", StringType, true),
      StructField("c_phone", StringType, true),
      StructField("c_acctbal", StringType, true),
      StructField("c_mktsegment", StringType, true),
      StructField("c_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val region_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\region.csv"
    val region_schema = StructType(Array(
      StructField("r_regionkey", StringType, true),
      StructField("r_name", StringType, true),
      StructField("r_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val nation_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\nation.csv"
    val nation_schema = StructType(Array(
      StructField("n_nationkey", StringType, true),
      StructField("n_name", StringType, true),
      StructField("n_regionkey", StringType, true),
      StructField("n_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val orders_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\orders.csv"
    val orders_schema = StructType(Array(
      StructField("o_orderkey", StringType, true),
      StructField("o_custkey", StringType, true),
      StructField("o_orderstatus", StringType, true),
      StructField("o_totalprice", StringType, true),
      StructField("o_orderdate", StringType, true),
      StructField("o_orderpriority", StringType, true),
      StructField("o_clerk", StringType, true),
      StructField("o_shippriority", StringType, true),
      StructField("o_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val lineitems_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\lineitem.csv"
    val lineitems_schema = StructType(Array(
      StructField("l_orderkey", StringType, true),
      StructField("l_partkey", StringType, true),
      StructField("l_suppkey", StringType, true),
      StructField("l_linenumber", StringType, true),
      StructField("l_quantity", StringType, true),
      StructField("l_extendedprice", StringType, true),
      StructField("l_discount", StringType, true),
      StructField("l_tax", StringType, true),
      StructField("l_returnflag", StringType, true),
      StructField("l_linestatus", StringType, true),
      StructField("l_shipdate", StringType, true),
      StructField("l_commitdate", StringType, true),
      StructField("l_receiptdate", StringType, true),
      StructField("l_shipinstruct", StringType, true),
      StructField("l_shipmode", StringType, true),
      StructField("l_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val part_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\part.csv"
    val part_schema = StructType(Array(
      StructField("p_partkey", StringType, true),
      StructField("p_name", StringType, true),
      StructField("p_mfgr", StringType, true),
      StructField("p_brand", StringType, true),
      StructField("p_type", StringType, true),
      StructField("p_size", StringType, true),
      StructField("p_container", StringType, true),
      StructField("p_retailprice", StringType, true),
      StructField("p_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val supplier_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\supplier.csv"
    val supplier_schema = StructType(Array(
      StructField("s_suppkey", StringType, true),
      StructField("s_name", StringType, true),
      StructField("s_address", StringType, true),
      StructField("s_nationkey", StringType, true),
      StructField("s_phone", StringType, true),
      StructField("s_acctbal", StringType, true),
      StructField("s_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val partsupp_raw = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\raw\\partsupp.csv"
    val partsupp_schema = StructType(Array(
      StructField("ps_partkey", StringType, true),
      StructField("ps_suppkey", StringType, true),
      StructField("ps_availqty", StringType, true),
      StructField("ps_supplycost", StringType, true),
      StructField("ps_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val customerDf = readData(spark,customer_raw,customer_schema)
    val regionDf = readData(spark,region_raw,region_schema)
    val nationDf = readData(spark,nation_raw,nation_schema)
    val ordersDf = readData(spark,orders_raw,orders_schema)
    val lineitemsDf = readData(spark,lineitems_raw,lineitems_schema)
    val partDf = readData(spark,part_raw,part_schema)
    val supplierDf = readData(spark,supplier_raw,supplier_schema)
    val partsuppDf = readData(spark,partsupp_raw,partsupp_schema)


    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\customer", customerDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\region", regionDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\nation", nationDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\order", ordersDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\lineitems", lineitemsDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\part", partDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\supplier", supplierDf)
    writeData(spark, s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal\\partsupp", partsuppDf)

    val denormDf = denormJoin(spark, customerDf,nationDf, regionDf, ordersDf, lineitemsDf, supplierDf, partDf, partsuppDf)
                   writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\denorm", denormDf)

    val starDf = star(spark, customerDf,nationDf, regionDf, ordersDf, lineitemsDf, supplierDf, partDf, partsuppDf)
    writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\customer", starDf(0))
    writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\lineitemorders", starDf(1))
    writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\supplier", starDf(2))
    writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\part", starDf(3))
    writeData(spark,s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\partsupp", starDf(4))
  }

  def readData(spark:SparkSession, dataPath: String,schemaOfCsv: StructType):DataFrame = {
    import spark.implicits._

    val data: DataFrame = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(schemaOfCsv)
      .load(dataPath)
      .drop($"last_col")

    //data.show(2, false)
    //println(data.count)
    return data
  }

  def writeData(spark:SparkSession, dataDestPath: String, data: DataFrame) = {
    data
      .repartition(1)
      .write
      .mode("Overwrite")
      .option("header", "false")
      .option("delimiter", "|")
      .format("csv")
      //.option("compression", "SNAPPY")
      .save(dataDestPath)
  }

  def denormJoin(spark:SparkSession, customer: DataFrame,nation: DataFrame, region: DataFrame, orders: DataFrame, lineitems: DataFrame,
                 supplier: DataFrame, part: DataFrame, partsupp: DataFrame):DataFrame = {

    import spark.implicits._

    val nation2 = nation
      .select(
        $"n_nationkey".as("n2_nationkey"),
        $"n_name".as("n2_name"),
        $"n_regionkey".as("n2_regionkey"),
        $"n_comment".as("n2_comment")
    )

    val region2 = region
      .select(
        $"r_regionkey".as("r2_regionkey"),
        $"r_name".as("r2.name"),
        $"r_comment".as("r2.comment")
      )

    val joined_df = customer
      .join(broadcast(nation), col("c_nationkey") === col("n_nationkey"), "left")
      .join(broadcast(region), col("n_regionkey") === col("r_regionkey"), "left")
      .join(orders, col("c_custkey") === col("o_custkey"), "left")
      .join(lineitems, col("o_orderkey") === col("l_orderkey"), "left")
      .join(supplier, col("l_suppkey") === col("s_suppkey"), "left")
      .join(broadcast(nation2), col("s_nationkey") === col("n2_nationkey"), "left")
      .join(broadcast(region2), col("n2_regionkey") === col("r2_regionkey"), "left")
      .join(part, lineitems.col("l_partkey") === part.col("p_partkey"), "left")
      .join(partsupp, (partsupp.col("ps_partkey") === part.col("p_partkey") && partsupp.col("ps_suppkey") === supplier.col("s_suppkey")), "left")

    //joined_df.show(2, false)
    //println(joined_df.count)
    return joined_df
  }

  def star(spark:SparkSession, customer: DataFrame,nation: DataFrame, region: DataFrame, orders: DataFrame, lineitems: DataFrame,
           supplier: DataFrame, part: DataFrame, partsupp: DataFrame): List[DataFrame] = {

    val joined_customer_Df = customer
      .join(broadcast(nation), col("c_nationkey") === col("n_nationkey"), "left")
      .join(broadcast(region), col("n_regionkey") === col("r_regionkey"), "left")

    val joined_lineitem_order__Df = orders
      .join(lineitems, col("o_orderkey") === col("l_orderkey"), "left")

    val joined_supplier_Df = supplier
      .join(broadcast(nation), col("s_nationkey") === col("n_nationkey"))
      .join(broadcast(region), col("n_regionkey") === col("r_regionkey"))

    val partsupp_df = partsupp
      .withColumn("ps_timestamp", current_timestamp())


    println(joined_customer_Df.count())
    println(joined_lineitem_order__Df.count())
    println(joined_supplier_Df.count())
    println(part.count())
    println(partsupp.count())

return List(joined_customer_Df,joined_lineitem_order__Df, joined_supplier_Df, part, partsupp_df)

  }


}