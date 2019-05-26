package com.masterarbeit.scala.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformationJob {

  def main(args: Array[String]) {

    //Create spark session
    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    //set spark configuration
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    spark.conf.set("spark.sql.orc.enabled",false)

    //Set scale factor and path for reading and writing the data
    val sf = 10 // can be 1,3 or 10
    val pathRaw = s"E:\\tpch\\data\\sf$sf\\raw"
    val pathWriteNormal = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\normal"
    val pathWriteDenormal = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\denormal\\"
    val pathWriteStar = s"C:\\Daten\\Projekte\\Masterarbeit\\tpch\\data\\sf$sf\\star\\"

    //schema for reading the customer csv file
    val customer_raw = s"$pathRaw\\customer.csv"
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

    //schema for reading the region csv file
    val region_raw = s"$pathRaw\\region.csv"
    val region_schema = StructType(Array(
      StructField("r_regionkey", StringType, true),
      StructField("r_name", StringType, true),
      StructField("r_comment", StringType, true),
      StructField("last_col", StringType, true)))

    //schema for reading the nation csv file
    val nation_raw = s"$pathRaw\\nation.csv"
    val nation_schema = StructType(Array(
      StructField("n_nationkey", StringType, true),
      StructField("n_name", StringType, true),
      StructField("n_regionkey", StringType, true),
      StructField("n_comment", StringType, true),
      StructField("last_col", StringType, true)))

    //schema for reading the orders csv file
    val orders_raw = s"$pathRaw\\orders.csv"
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

    //schema for reading the lineitems csv file
    val lineitems_raw = s"$pathRaw\\lineitem.csv"
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

    //schema for reading the part csv file
    val part_raw = s"$pathRaw\\part.csv"
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

    //schema for reading the supplier csv file
    val supplier_raw = s"$pathRaw\\supplier.csv"
    val supplier_schema = StructType(Array(
      StructField("s_suppkey", StringType, true),
      StructField("s_name", StringType, true),
      StructField("s_address", StringType, true),
      StructField("s_nationkey", StringType, true),
      StructField("s_phone", StringType, true),
      StructField("s_acctbal", StringType, true),
      StructField("s_comment", StringType, true),
      StructField("last_col", StringType, true)))

    //schema for reading the partsupp csv file
    val partsupp_raw = s"$pathRaw\\partsupp.csv"
    val partsupp_schema = StructType(Array(
      StructField("ps_partkey", StringType, true),
      StructField("ps_suppkey", StringType, true),
      StructField("ps_availqty", StringType, true),
      StructField("ps_supplycost", StringType, true),
      StructField("ps_comment", StringType, true),
      StructField("last_col", StringType, true)))

    //call functions to read the source data
    val customerDf = readData(spark,customer_raw,customer_schema)
    val regionDf = readData(spark,region_raw,region_schema)
    val nationDf = readData(spark,nation_raw,nation_schema)
    val ordersDf = readData(spark,orders_raw,orders_schema)
    val lineitemsDf = readData(spark,lineitems_raw,lineitems_schema)
    val partDf = readData(spark,part_raw,part_schema)
    val supplierDf = readData(spark,supplier_raw,supplier_schema)
    val partsuppDf = readData(spark,partsupp_raw,partsupp_schema)


    //write data for third normal schema
    writeData(spark, s"$pathWriteNormal\\customer", customerDf)
    writeData(spark, s"$pathWriteNormal\\region", regionDf)
    writeData(spark, s"$pathWriteNormal\\nation", nationDf)
    writeData(spark, s"$pathWriteNormal\\order", ordersDf)
    writeData(spark, s"$pathWriteNormal\\lineitems", lineitemsDf)
    writeData(spark, s"$pathWriteNormal\\part", partDf)
    writeData(spark, s"$pathWriteNormal\\supplier", supplierDf)
    writeData(spark, s"$pathWriteNormal\\partsupp", partsuppDf)

    //call function to denormal the third normal data and write the data to the file system
    val denormDf = denormJoin(spark, customerDf,nationDf, regionDf, ordersDf, lineitemsDf, supplierDf, partDf, partsuppDf)
    writeData(spark,s"$pathWriteDenormal", denormDf)

    //call function to trasnform the normal data to the star schema and write the data to the file system
    val starDf = star(spark, customerDf,nationDf, regionDf, ordersDf, lineitemsDf, supplierDf, partDf, partsuppDf)
    writeData(spark,s"$pathWriteStar\\customer", starDf(0))
    writeData(spark,s"$pathWriteStar\\lineitemorders", starDf(1))
    writeData(spark,s"$pathWriteStar\\supplier", starDf(2))
    writeData(spark,s"$pathWriteStar\\part", starDf(3))
    writeData(spark,s"$pathWriteStar\\partsupp", starDf(4))
  }

  //function for reading the raw csv files
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

  //function to write the transformed data to the file system
  def writeData(spark:SparkSession, dataDestPath: String, data: DataFrame) = {

    data
      .na.fill("")
      //.repartition(1)
      .write
      .mode("Overwrite")
      .option("header", "false")
      .option("delimiter", "|")
      .format("csv")
      //.option("compression", "SNAPPY")
      .save(dataDestPath)
  }

  //function to transform the normal data to the denormalized schema
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
        $"r_name".as("r2_name"),
        $"r_comment".as("r2_comment")
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
      .drop($"n_comment").drop($"r_comment").drop($"o_comment").drop($"l_comment")
      .drop($"n2_comment").drop($"r2_comment").drop($"p_comment").drop($"ps_comment")

    return joined_df
  }

  //function to transform the normal data to the star schema
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

return List(joined_customer_Df,joined_lineitem_order__Df, joined_supplier_Df, part, partsupp_df)

  }


}
