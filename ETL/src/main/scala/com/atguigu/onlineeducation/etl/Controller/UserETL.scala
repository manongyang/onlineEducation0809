package com.atguigu.onlineeducation.etl.Controller

import com.alibaba.fastjson.JSON
import com.atguigu.onlineeducation.etl.bean.MemberLog
import org.apache.spark.sql.SparkSession

object UserETL {
  def main(args: Array[String]): Unit = {



/*    val conf=new SparkConf().setMaster("local[*]").setAppName("Demo")

    val sc = new SparkContext(conf)*/

    System.setProperty("HADOOP_USER_NAME" , "atguigu")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Demo")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val sc = spark.sparkContext



    //{"ad_id":"8","birthday":"1960-01-26","dn":"webA","dt":"20190722","email":"test@126.com","fullname":"王97459"
    // ,"iconurl":"-","lastlogin":"-","mailaddr":"-","memberlevel":"1","password":"123456"
    // ,"paymoney":"-","phone":"13711235451","qq":"10000","register":"2018-02-19","regupdatetime":"-"
    // ,"uid":"97459","unitname":"-","userip":"106.81.219.35","zipcode":"-"}


    //插入member表

    val sourceRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/member.log")


    val filterRDD = sourceRDD.filter(line => line.startsWith("{")&&line.endsWith("}"))

    val UserRdd = filterRDD.map(line => {
      JSON.parseObject(line, classOf[MemberLog])
    })

    .map(user => {

      user.fullname = user.fullname.substring(0,1) + "XX"

      user.phone = user.phone.substring(0,3) + "*****" + user.phone.substring(8)

      user.password = "******"




      user.birthDay= user.birthDay.replaceAll("-" , "")

      user.register = user.register.replaceAll("-" , "")


      user
    })



    spark.sql("use dwd")

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val userDS = UserRdd.toDS().write.mode("overwrite").insertInto("dwd_member")


    spark.stop()


  }
}
