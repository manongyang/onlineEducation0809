package com.atguigu.onlineeducation.etl.bean

case class MemberLog(uid: Int, adId: Int, var birthDay: String, email: String,
                     var fullname: String, iconUrl: String, lastLogin: String, mailAddr: String,
                     memberlevel: String, var password: String, payMoney: String,
                     var phone: String, qq: String, var register: String, var regUpdateTime: String,
                     unitName: String, userIp: String, zipCode: String, dt: String, dn: String)

case class BaseAdLog(adId: Int, adName: String, dn: String)


case class BaseWebSiteLog(siteId: Int, siteName: String, siteUrl: String, delete: Int, var createTime: String, creator: String, dn: String)


case class MemberRegtype(uid: Int, appKey: String, appRegUrl: String, bdpUUID: String, createTime: String
                         , domain: String, isRanReg: String, regSource: String, resourceName: String, webSiteid: Int, dt: String, dn: String)

case class PcenternMemPayMoneyLog(uid: Int, payMoney: String, siteId: Int, vipId: Int, dt: String, dn: String)


case class PcenternMemVipLevelLog(vipId: Int, vipLevel: String, startTime: String, endTime: String, lastModifyTime: String, maxFree: String,
                                  minFree: String, nextLevel: String, operator: String, dn: String)
