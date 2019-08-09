package com.atguigu.onlineeducation.etl.util

import com.alibaba.fastjson.JSON
import com.atguigu.onlineeducation.etl.bean.MemberLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Util {



    def repalceDate(date: String) = {

      date.replaceAll("-", "")
    }


  /*

  def FileToRDD(path: String , clazz : String) = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Demo")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val sc = spark.sparkContext
    val sourceRDD = sc.textFile(path)

    val userClass = Class.forName(clazz)


    val filterRDD = sourceRDD.filter(line => line.startsWith("{") && line.endsWith("}"))

    val UserRdd = filterRDD.map(line => {
      JSON.parseObject(line, userClass.getClass)

    })



    RDDToHive(spark , UserRdd , "dwd.")




    spark.stop()

  }

  def RDDToHive ( spark : SparkSession ,sss : Dataset[Any] , tableName : String): Unit ={

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val userDS = sss.write.mode("overwrite").insertInto(tableName)

  }*/


}
