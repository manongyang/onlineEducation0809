package com.atguigu.onlineeducation.etl.Controller

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.onlineeducation.etl.bean._
import com.atguigu.onlineeducation.etl.util.Util
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {


    /*    val conf=new SparkConf().setMaster("local[*]").setAppName("Demo")

        val sc = new SparkContext(conf)*/

    System.setProperty("HADOOP_USER_NAME", "atguigu")

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



    //2.插入baseWeb表
    val sourceRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baswewebsite.log")


    val filterRDD = sourceRDD.filter(line => line.startsWith("{") && line.endsWith("}"))


    val UserDS = filterRDD.map(line => {
      JSON.parseObject(line, classOf[BaseWebSiteLog])
    }).toDS()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    UserDS.write.mode("overwrite").insertInto("dwd.dwd_base_website")

    //3.插入baselog


    val baseLogRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baseadlog.log")


    val baseLogfilterRDD = baseLogRDD.filter(line => line.startsWith("{") && line.endsWith("}"))


    val baseLogfilterDS = baseLogfilterRDD.map(line => {
      JSON.parseObject(line, classOf[BaseAdLog])
    }).toDS()


    baseLogfilterDS.write.mode("overwrite").insertInto("dwd.dwd_base_ad")


    //4.插入memberRegtype表

    val memberRegTypeRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/memberRegtype.log")


    val memberRegFilterTypeRDD = memberRegTypeRDD.filter(line => line.startsWith("{") && line.endsWith("}"))


    val memberRegTypefilterDS = memberRegFilterTypeRDD.map(line => {

      val rawObj = JSON.parseObject(line)
      rawObj.getString("regsource") match
        {
        case "1" => rawObj.put("resourceName" , "PC")
        case "2" => rawObj.put("resourceName" , "MOBILE")
        case "3" => rawObj.put("resourceName" , "APP")
        case "4" => rawObj.put("resourceName" , "WECHAT")
        case _ => rawObj.put("resourceName" , "other")
      }


      JSON.parseObject(rawObj.toJSONString, classOf[MemberRegtype])

    }).toDS()


    memberRegTypefilterDS.write.mode("overwrite").insertInto("dwd.dwd_member_regtype")


    //5.插入pcenterMemVipLevel表

    val pcenterMemVipLevelRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcenterMemViplevel.log")


    val pcenterMemVipLevelFilterTypeRDD = pcenterMemVipLevelRDD.filter(line => line.startsWith("{") && line.endsWith("}"))


    val pcenterMemVipLevelfilterDS = pcenterMemVipLevelFilterTypeRDD.map(line => {
      val rawObj = JSON.parseObject(line)
      rawObj.remove("discountval")

      JSON.parseObject(rawObj.toJSONString , classOf[PcenternMemVipLevelLog])
    }).toDS()


    pcenterMemVipLevelfilterDS.write.mode("overwrite").insertInto("dwd.dwd_vip_level")


    //6.插入	pcenterMemPayMoney表


    val pcenterMemPayMoneyRDD = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcentermempaymoney.log")


    val pcenterMemPayMoneyFilterTypeRDD = pcenterMemPayMoneyRDD.filter(line => line.startsWith("{") && line.endsWith("}"))


    val pcenterMemPayMoneyfilterDS = pcenterMemPayMoneyFilterTypeRDD.map(line => {
      JSON.parseObject(line, classOf[PcenternMemPayMoneyLog])
    }).toDS()


    pcenterMemPayMoneyfilterDS.write.mode("overwrite").insertInto("dwd.dwd_pcentermempaymoney")


    spark.stop()

  }

}
