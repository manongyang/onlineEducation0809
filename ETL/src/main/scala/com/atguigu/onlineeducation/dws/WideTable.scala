package com.atguigu.onlineeducation.dws

import org.apache.spark.sql.SparkSession

object WideTable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Demo")
      .enableHiveSupport()
      .getOrCreate()


    val sql =
      """
        |select
        |m.uid,m.ad_id,m.fullname,m.iconurl,m.lastlogin,m.mailaddr,m.memberlevel,m.password,m.paymoney,m.phone,m.qq,m.register,m.regupdatetime,m.unitname,m.zipcode,
        |r.appkey,r.appregurl,r.bdp_uuid,r.createtime,r.domain,r.isranreg,r.regsource,
        |a.adname,
        |w.siteid,w.sitename,w.siteurl,w.`delete`,w.createtime,w.creator,
        |p.vip_id,l.vip_level,
        |l.start_time,l.end_time,l.last_modify_time,l.max_free,l.min_free,l.next_level,l.operator
        |
        |from dwd_member  m
        |left join dwd_member_regtype r left join dwd_base_ad a left join dwd_base_website w
        |left join dwd_pcentermempaymoney p left join dwd_vip_level l
        |on m.uid = r.uid  and m.ad_id = a.adid and  m.uid = p.uid and p.siteid  = w.siteid and p.vip_id = l.vip_id
      """.stripMargin

    spark.sql(sql).show()

    spark.stop()


  }

}
