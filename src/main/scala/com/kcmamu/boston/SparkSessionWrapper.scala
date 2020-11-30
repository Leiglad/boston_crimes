package com.kcmamu.boston

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator


trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("boston crimes").getOrCreate()
  }

}

trait SparkSessionWrapperGeo extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      appName("boston crimes").getOrCreate()

  }

}