package com.kcmamu.boston
import java.lang.reflect.Array

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

import scala.collection.JavaConversions._


class DistrictFiller(df : DataFrame) extends SparkSessionWrapperGeo {

  import spark.implicits._
  //конструктрор
  df.createOrReplaceTempView("geo_things")
  GeoSparkSQLRegistrator.registerAll(spark.sqlContext)
  private val spatialDf = spark.sql(
    """SELECT ST_Point(CAST(geo_things.Lat AS Decimal(24,20)),CAST(geo_things.Long AS Decimal(24,20)))
      |AS geo_loc FROM geo_things""".stripMargin)
  private val spatialRDD = Adapter.toSpatialRdd(spatialDf,"geo_loc" )
  spatialRDD.buildIndex(IndexType.RTREE, false)
  val geometryFactory = new GeometryFactory()
  val df_with_dist: DataFrame = df.na.drop(Seq("DISTRICT")).cache()



  // Заполнение отсутствующего района по координатам, наивный подход
  def df_with_filled_dists(df: DataFrame, debugMode : Boolean = false): DataFrame = {

    //датафрейм с в вычещенными координатами координатами
    val df_coord_clear = df.filter("42 < Lat AND Lat < 43 AND -70.5 > Long AND Long > -71.5")
    val df_other = df.exceptAll(df_coord_clear)


    //находим границы каждого района
    val dist_bounds = df_coord_clear.na.drop("all", Seq("DISTRICT"))
      .agg(
        min("Lat").as("minLat"),
        min("Long").as("minLong"),
        max("Lat").as("maxLat"),
        max("Long").as("maxLong")
      )

    val dist_boundsM: Seq[Map[String, Any]] = dist_bounds.collect().map(r => Map(dist_bounds.columns.zip(r.toSeq): _*))

    //udf District filler
    val distChoser: (Double, Double) => Option[String] = (Lat, Long) => {
      val matchingDist: (Option[Any], Option[Any], Option[Any], Option[Any]) => Boolean = (minLat, minLong, maxLat, maxLong) => {
        (minLat, minLong, maxLat, maxLong) match {
          case  (Some(minLat: Double), Some(minLong: Double), Some(maxLat: Double), Some(maxLong: Double)) =>
            minLat < Lat && Lat < maxLat && minLong < Long && Long < maxLong
          case _ => false
        }
      }
      var res : Option[String] = null
      val it = dist_boundsM.iterator
      while (res == null && it.hasNext) {
        val row = it.next()
        //println(getClassName((row.get("minLat"), row.get("minLong"), row.get("maxLat"), row.get("maxLong"))))
        if (matchingDist(row.get("minLat"), row.get("minLong"), row.get("maxLat"), row.get("maxLong"))) {
          res = row.get("DISTRICT").asInstanceOf[Option[String]]
        }
      }
      res
    }

    val distChoserUDF = udf(distChoser)

    //print test dataset for check
    if (debugMode) {
      val testdataset = df_coord_clear.where("DISTRICT is null")
      val testfilled = testdataset.withColumn("DISTRICT", distChoserUDF(col("Lat"), col("Long")))
      dist_bounds.show(false)
      testdataset.show(20,false)
      testfilled.show(20,false)
    }
    val df_with_filled_dists = df_coord_clear.withColumn("DISTRICT",
      when($"DISTRICT" === null, distChoserUDF(col("Lat"), col("Long")))
    )
    val RES = df_with_filled_dists.union(df_other)

    if (debugMode) {
      assert(RES.count() == df.count())
    }

    RES
  }




  private val getDist: (java.util.List[Geometry]) => String = points => {
    val pointMatches: (Double, Double) => Boolean = (lat, lon) => {
      val p = geometryFactory.createPoint(new Coordinate(lat, lon))
      points.contains(p)
    }
    val pointMatchesUDF = udf(pointMatches)

    val dists = df_with_dist.select(col("DISTRICT"), $"Lat" , $"Long")
      .filter(pointMatchesUDF($"Lat", $"Long"))
      .groupBy("DISTRICT").count().sort("count")

    dists.head().getAs[String]("DISTRICT")
  }


  // назождение отсутствующего района по координатам с использованием
  // геоспарка - находится 10 ближайших точек из датасета с заполненными районами, далее выбирается район в который входит большинство найденных точек
  private val findDistrict: (Double, Double) => String = (Lat, Long) => {
    val numKPoints = 10
    val point = geometryFactory.createPoint(new Coordinate(Lat, Long))
    val nearest = KNNQuery.SpatialKnnQuery(spatialRDD, point, numKPoints, true)
    getDist(nearest)
  }



  def fillDists(limit: Int ) : DataFrame = {

    // выбираем незаполненные
    var  df_null_dist = df.filter(df("DISTRICT").isNull || df("DISTRICT") === "" || df("DISTRICT").isNaN)
                         .limit(limit)
    val df_other = df.exceptAll(df_null_dist)

    // добавляем id чтоб потом проще сждойнить обратно,
    df_null_dist = df_null_dist.withColumn("id",monotonically_increasing_id())

    // находим район
    val mp  = df_null_dist.withColumn("id", monotonically_increasing_id()).collect.map(
      (row: Row) => {
        val lat = row.getAs[Double]("Lat")
        val long = row.getAs[Double]("Long")
        val dist = findDistrict(lat, long)
        val id = row.getAs[Long]("id")
        Row(id, dist)
      })
    val rdd = spark.sparkContext.makeRDD(mp)

    val schema = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("fDISTRICT", StringType, true)
      )
    )
    val filled = spark.createDataFrame(rdd, schema)
    df_null_dist = df_null_dist.join(filled, "id")
      .withColumn("DISTRICT", coalesce($"DISTRICT",$"fDISTRICT"))
      .drop("id","fDISTRICT")
    df_null_dist.show()
    df_other.union(df_null_dist)

  }

}
