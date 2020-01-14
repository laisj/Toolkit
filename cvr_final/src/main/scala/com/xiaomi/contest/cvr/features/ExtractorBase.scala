package com.xiaomi.contest.cvr.features

import com.xiaomi.contest.cvr.group.FeaGroup
import com.xiaomi.contest.cvr.samples.{BaseFea, Sample}
import com.xiaomi.contest.cvr.utils.DataUtils
import com.xiaomi.contest.cvr.utils.FeaUtils._
import com.xiaomi.contest.cvr.{CVRInstance, DatasetHelper, _}
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object ExtractorBase {
  final private val cat = "_"

  def extractFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()

    val data = instance.getData
    val ad = instance.getAd
    val profile = instance.getProfile
    val app_category = instance.getApp_category

    val advertiser_id = ad.getAdvertiser_id.toString
    val campaign_id = ad.getCampaign_id.toString
    val ad_id = ad.getAd_id.toString
    val app_id = ad.getApp_id.toString

    val age = profile.getAge.toString
    val gender = profile.getGender.toString
    val education = profile.getEducation
    val province = profile.getProvince.toString
    val city = profile.getCity.toString
    val device_info = profile.getDevice_info.toString

    val app_description = app_category.getApp_description
    val app_category1 = app_category.getApp_category1.toString
    val app_category2 = app_category.getApp_category2.toString

    val pos = data.getPosition_id.toString
    val connection_type = data.getConnection_type
    val miui_version = data.getMiui_version.toString
    val user_id = data.getUser_id.toString
    val ip = data.getIp.toString
    val android_version = data.getAndroid_version.toString

    val click_time = data.getClick_time.toInt
    val clk_day = ((click_time / 10000) % 7).toString
    val clk_hour = ((click_time % 10000) / 100).toString

    features :+= makeFea(FeaGroup.clk_day, clk_day)
    features :+= makeFea(FeaGroup.clk_day_pos, clk_day + cat + pos)
    features :+= makeFea(FeaGroup.clk_day_connection_type, clk_day + cat + connection_type)

    features :+= makeFea(FeaGroup.clk_day_advertiser_id, clk_day + cat + advertiser_id)
    features :+= makeFea(FeaGroup.clk_day_campaign_id, clk_day + cat + campaign_id)
    features :+= makeFea(FeaGroup.clk_day_ad_id, clk_day + cat + ad_id)
    features :+= makeFea(FeaGroup.clk_day_app_id, clk_day + cat + app_id)
    features :+= makeFea(FeaGroup.clk_day_app_category1, clk_day + cat + app_category1)
    features :+= makeFea(FeaGroup.clk_day_app_category2, clk_day + cat + app_category2)

    features :+= makeFea(FeaGroup.clk_hour, clk_hour)
    features :+= makeFea(FeaGroup.clk_hour_pos, clk_hour + cat + connection_type)
    features :+= makeFea(FeaGroup.clk_hour_connection_type, clk_hour + cat + connection_type)

    features :+= makeFea(FeaGroup.clk_hour_advertiser_id, clk_hour + cat + advertiser_id)
    features :+= makeFea(FeaGroup.clk_hour_campaign_id, clk_hour + cat + campaign_id)
    features :+= makeFea(FeaGroup.clk_hour_ad_id, clk_hour + cat + ad_id)
    features :+= makeFea(FeaGroup.clk_hour_app_id, clk_hour + cat + app_id)
    features :+= makeFea(FeaGroup.clk_hour_app_category1, clk_hour + cat + app_category1)
    features :+= makeFea(FeaGroup.clk_hour_app_category2, clk_hour + cat + app_category2)

    features :+= makeFea(FeaGroup.pos, pos)
    features :+= makeFea(FeaGroup.pos_advertiser_id, pos + cat + advertiser_id)
    features :+= makeFea(FeaGroup.pos_campaign_id, pos + cat + campaign_id)
    features :+= makeFea(FeaGroup.pos_ad_id, pos + cat + ad_id)
    features :+= makeFea(FeaGroup.pos_app_id, pos + cat + app_id)
    features :+= makeFea(FeaGroup.pos_app_category1, pos + cat + app_category1)
    features :+= makeFea(FeaGroup.pos_app_category2, pos + cat + app_category2)

    features :+= makeFea(FeaGroup.pos_miui_version, pos + cat + miui_version)
    features :+= makeFea(FeaGroup.pos_android_version, pos + cat + android_version)
    features :+= makeFea(FeaGroup.pos_connection_type, pos + cat + connection_type)

    features :+= makeFea(FeaGroup.pos_age, pos + cat + age)
    features :+= makeFea(FeaGroup.pos_gender, pos + cat + gender)
    features :+= makeFea(FeaGroup.pos_education, pos + cat + education)
    features :+= makeFea(FeaGroup.pos_province, pos + cat + province)
    features :+= makeFea(FeaGroup.pos_city, pos + cat + city)
    features :+= makeFea(FeaGroup.pos_device_info, pos + cat + device_info)

    features :+= makeFea(FeaGroup.pos_age_gender, pos + cat + age + cat + gender)

    // features :+= makeFea(FeaGroup.user_id, user_id)
    // features :+= makeFea(FeaGroup.ip, ip)

    features :+= makeFea(FeaGroup.miui_version, miui_version)
    features :+= makeFea(FeaGroup.android_version, android_version)

    features :+= makeFea(FeaGroup.connection_type, connection_type)
    features :+= makeFea(FeaGroup.connection_type_advertiser_id, connection_type + cat + advertiser_id)
    features :+= makeFea(FeaGroup.connection_type_campaign_id, connection_type + cat + campaign_id)
    features :+= makeFea(FeaGroup.connection_type_ad_id, connection_type + cat + ad_id)
    features :+= makeFea(FeaGroup.connection_type_app_id, connection_type + cat + app_id)
    features :+= makeFea(FeaGroup.connection_type_app_category1, connection_type + cat + app_category1)
    features :+= makeFea(FeaGroup.connection_type_app_category2, connection_type + cat + app_category2)

    features :+= makeFea(FeaGroup.connection_type_miui_version, connection_type + cat + miui_version)
    features :+= makeFea(FeaGroup.connection_type_android_version, connection_type + cat + android_version)

    features :+= makeFea(FeaGroup.connection_type_age, connection_type + cat + age)
    features :+= makeFea(FeaGroup.connection_type_gender, connection_type + cat + gender)
    features :+= makeFea(FeaGroup.connection_type_education, connection_type + cat + education)
    features :+= makeFea(FeaGroup.connection_type_province, connection_type + cat + province)
    features :+= makeFea(FeaGroup.connection_type_city, connection_type + cat + city)
    features :+= makeFea(FeaGroup.connection_type_device_info, connection_type + cat + device_info)

    features :+= makeFea(FeaGroup.connection_type_age_gender, connection_type + cat + age + cat + gender)

    features :+= makeFea(FeaGroup.advertiser_id, advertiser_id)
    features :+= makeFea(FeaGroup.campaign_id, campaign_id)
    features :+= makeFea(FeaGroup.ad_id, ad_id)
    features :+= makeFea(FeaGroup.app_id, app_id)
    features :+= makeFea(FeaGroup.app_category1, app_category1)
    features :+= makeFea(FeaGroup.app_category2, app_category2)

    features :+= makeFea(FeaGroup.age_gender, age + cat + gender)
    features :+= makeFea(FeaGroup.age_gender_advertiser_id, age + cat + gender + cat + advertiser_id)
    features :+= makeFea(FeaGroup.age_gender_campaign_id, age + cat + gender + cat + campaign_id)
    features :+= makeFea(FeaGroup.age_gender_ad_id, age + cat + gender + cat + ad_id)
    features :+= makeFea(FeaGroup.age_gender_app_id, age + cat + gender + cat + app_id)
    features :+= makeFea(FeaGroup.age_gender_app_category1, age + cat + gender + cat + app_category1)
    features :+= makeFea(FeaGroup.age_gender_app_category2, age + cat + gender + cat + app_category2)

    features :+= makeFea(FeaGroup.age, age)
    features :+= makeFea(FeaGroup.age_advertiser_id, age + cat + advertiser_id)
    features :+= makeFea(FeaGroup.age_campaign_id, age + cat + campaign_id)
    features :+= makeFea(FeaGroup.age_ad_id, age + cat + ad_id)
    features :+= makeFea(FeaGroup.age_app_id, age + cat + app_id)
    features :+= makeFea(FeaGroup.age_app_category1, age + cat + app_category1)
    features :+= makeFea(FeaGroup.age_app_category2, age + cat + app_category2)

    features :+= makeFea(FeaGroup.gender, gender)
    features :+= makeFea(FeaGroup.gender_advertiser_id, gender + cat + advertiser_id)
    features :+= makeFea(FeaGroup.gender_campaign_id, gender + cat + campaign_id)
    features :+= makeFea(FeaGroup.gender_ad_id, gender + cat + ad_id)
    features :+= makeFea(FeaGroup.gender_app_id, gender + cat + app_id)
    features :+= makeFea(FeaGroup.gender_app_category1, gender + cat + app_category1)
    features :+= makeFea(FeaGroup.gender_app_category2, gender + cat + app_category2)

    features :+= makeFea(FeaGroup.education, education)
    features :+= makeFea(FeaGroup.education_advertiser_id, education + cat + advertiser_id)
    features :+= makeFea(FeaGroup.education_campaign_id, education + cat + campaign_id)
    features :+= makeFea(FeaGroup.education_ad_id, education + cat + ad_id)
    features :+= makeFea(FeaGroup.education_app_id, education + cat + app_id)
    features :+= makeFea(FeaGroup.education_app_category1, education + cat + app_category1)
    features :+= makeFea(FeaGroup.education_app_category2, education + cat + app_category2)

    features :+= makeFea(FeaGroup.province, province)
    features :+= makeFea(FeaGroup.province_advertiser_id, province + cat + advertiser_id)
    features :+= makeFea(FeaGroup.province_campaign_id, province + cat + campaign_id)
    features :+= makeFea(FeaGroup.province_ad_id, province + cat + ad_id)
    features :+= makeFea(FeaGroup.province_app_id, province + cat + app_id)
    features :+= makeFea(FeaGroup.province_app_category1, province + cat + app_category1)
    features :+= makeFea(FeaGroup.province_app_category2, province + cat + app_category2)

    features :+= makeFea(FeaGroup.city, city)
    features :+= makeFea(FeaGroup.city_advertiser_id, city + cat + advertiser_id)
    features :+= makeFea(FeaGroup.city_campaign_id, city + cat + campaign_id)
    features :+= makeFea(FeaGroup.city_ad_id, city + cat + ad_id)
    features :+= makeFea(FeaGroup.city_app_id, city + cat + app_id)
    features :+= makeFea(FeaGroup.city_app_category1, city + cat + app_category1)
    features :+= makeFea(FeaGroup.city_app_category2, city + cat + app_category2)

    features :+= makeFea(FeaGroup.device_info, device_info)
    features :+= makeFea(FeaGroup.device_info_advertiser_id, device_info + cat + advertiser_id)
    features :+= makeFea(FeaGroup.device_info_campaign_id, device_info + cat + campaign_id)
    features :+= makeFea(FeaGroup.device_info_ad_id, device_info + cat + ad_id)
    features :+= makeFea(FeaGroup.device_info_app_id, device_info + cat + app_id)
    features :+= makeFea(FeaGroup.device_info_app_category1, device_info + cat + app_category1)
    features :+= makeFea(FeaGroup.device_info_app_category2, device_info + cat + app_category2)

    features
  }
}
