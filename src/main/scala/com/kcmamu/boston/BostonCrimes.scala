package com.kcmamu.boston

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import scala.reflect.runtime.universe._

object BostonCrimes extends App with SparkSessionWrapperGeo {

  import spark.implicits._

  def getClassName[T: TypeTag](input: T): String = {
    typeOf[T].toString
  }

  //Убираем Notice из вывода чтоб видно было результаты
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("GeoSparkKryoRegistrator").setLevel(Level.ERROR)

  Logger.getLogger(classOf[GeoSparkKryoRegistrator]).setLevel(Level.ERROR)

  // * crimes_total - общее количество преступлений в этом районе
  def crimes_total(df_dist_nn: DataFrame): DataFrame = {
    val CRIMES_TOTAL = df_dist_nn.groupBy("DISTRICT").count()
    CRIMES_TOTAL
  }


  def crimes_monthly(df_dist_nn: DataFrame): DataFrame = {
    //функиция для расчета медианы
    def median(inputList: Seq[Long]): Double = {
      val count = inputList.size
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (inputList(l) + inputList(r)).toDouble / 2
      } else
        inputList(count / 2).toDouble
    }

    val by_dist_monthly = df_dist_nn.groupBy('MONTH, 'YEAR, 'DISTRICT)
      .count()
      .groupBy('DISTRICT)
      .agg(collect_list("count").alias("all_crimes"))
      .select('DISTRICT, sort_array('all_crimes).as("all_crimes"))

    // * crimes_monthly - медиана числа преступлений в месяц в этом районе
    val CRIMES_MONTHLY = by_dist_monthly.select("DISTRICT", "all_crimes")
      .rdd.map({ m => (m.getString(0), median(m.getAs[Seq[Long]](1))) })
      .toDF("DISTRICT", "median")
    CRIMES_MONTHLY
  }


  // * crimes_total - общее количество преступлений в этом районе
  def frequent_crime_types(df_dist_nn: DataFrame, df_codes: DataFrame): DataFrame = {
    // ##############################frequent_crime_types
    //собираем датафрейм с тремя самыми частыми преступлениями идущими по порядку для всех районов(сгруппированных)
    val ranked = df_dist_nn.groupBy('DISTRICT, 'OFFENSE_CODE)
      .count()
      .withColumn("rank", dense_rank()
        .over(Window.partitionBy('DISTRICT).orderBy(desc("count"))))
      .filter(col("rank") <= 3)


    //я не знаю как быть с дублирющимися наименованиями - объеденил через '|||'
    val mkShortString: Seq[String] => String = _.map(_.split("-").map(_.trim).head)
      .mkString(" ||| ")
    val mkShortStringUDF = udf(mkShortString)

    val df_codes_cl = df_codes.distinct().groupBy("CODE")
      .agg(collect_list("NAME").as("NAME"))
      .withColumn("NAME", mkShortStringUDF('NAME))


    // * frequent_crime_types - три самых частых crime_type за всю историю наблюдений
    val FREQUENT_CRIME_TYPES = ranked.join(df_codes_cl).where($"OFFENSE_CODE" === $"CODE")
      .distinct()
      .groupBy("DISTRICT")
      .agg(concat_ws(", ", collect_list("NAME")).as("frequent_crime_types"))

    FREQUENT_CRIME_TYPES
  }


  // * crimes_total - общее количество преступлений в этом районе
  def avgLatLong(df: DataFrame): DataFrame = {

    val grouped = df.groupBy("DISTRICT")
    val avgLat = grouped.agg(avg("Lat"))
    val avgLong = grouped.agg(avg("Long"))

    avgLat.join(avgLong, "DISTRICT")
  }


  //чтение исходных данных
  val df = spark.read.format("csv").
    option("header", "true").
    option("inferSchema", "true").
    load("../../data/crime.csv")

  var df_codes = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("../../data/offense_codes.csv")



  //not na DISTRICT
  val df_nna_dist = df.na.drop("all", Seq("DISTRICT")).cache()


  val CRIMES_TOTAL = crimes_total(df_nna_dist)
  println("-------   crimes_total - общее количество преступлений в этом районе   -----------")
  CRIMES_TOTAL.show(100, false)
  CRIMES_TOTAL.unpersist()

  println("-------   crimes_monthly - медиана числа преступлений в месяц в этом районе   ---------")
  val CRIMES_MONTHLY = crimes_monthly(df_nna_dist)
  CRIMES_MONTHLY.show(100, false)
  CRIMES_MONTHLY.unpersist()

  println("-------   frequent_crime_types - три самых частых crime_type за всю историю наблюдений   --------- \n" +
          "          (наименования прступлений с  одинаковым кодом разделены последовательностью '|||')       ")
  val FREQUENT_CRIME_TYPES = frequent_crime_types(df_nna_dist, df_codes)
  FREQUENT_CRIME_TYPES.show(100, false)
  FREQUENT_CRIME_TYPES.unpersist()

  println("--------   lat - широта координаты района,lon - долгота координаты района, расчитанная как среднее по всем широтам инцидентов   ---------")
  val AVGLATLONG = avgLatLong(df_nna_dist.filter("42 < Lat AND Lat < 43 AND -70.5 > Long AND Long > -71.5"))
  AVGLATLONG.show(100,false)
  AVGLATLONG.unpersist()





//  //эксперименты
  val DF = new DistrictFiller(df.filter("42 < Lat AND Lat < 43 AND -70.5 > Long AND Long > -71.5"))
  val filled = DF.fillDists(3)








}

