package com.xiaomi.contest.cvr.features

import com.xiaomi.contest.cvr.group.FeaGroup
import com.xiaomi.contest.cvr.samples.{BaseFea, Sample}
import com.xiaomi.contest.cvr.utils.FeaUtils._
import com.xiaomi.contest.cvr.{CVRInstance, Counter, DatasetHelper}

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object ExtractorLrFea {
  final private val cat = "_"

  def single(instance: CVRInstance, hash: DatasetHelper, idx: Int, sameCount: Int): Sample = {
    val treeFeatures = extractTreeFeatures(instance)
    val sameSampleFeatures = extractSameSampleFeatures(instance, idx, sameCount)
    val counterFeatures = extractCounterFeatures(instance)
    val features = extractFeatures(instance, hash)
    val ans = sameSampleFeatures ++ treeFeatures ++ counterFeatures ++ features
    makeSample(ans, instance)
  }

  def single(instance: CVRInstance, hash: DatasetHelper): Sample = {
    val treeFeatures = extractTreeFeatures(instance)
    makeSample(treeFeatures, instance)
  }

  def getLabel(cnt: Counter): String = {
    (cnt.getLabel / 100).toString
  }

  def getCVR(cnt: Counter): String = {
    ((1 + cnt.getLabel) * 100 / (1 + cnt.getClk)).toString
  }

  private def extractTreeFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    if (instance.isSetTree) {
      val leaf = instance.getTree.getLeaf_indices
      for (num <- leaf.indices) {
        features :+= makeFea(FeaGroup.leaf_indices, num + "_" + leaf.get(num))
      }
    }
    features
  }

  private def extractSameSampleFeatures(instance: CVRInstance, idx: Int, sameCount: Int): List[BaseFea] = {
    var features = List[BaseFea]()
    if (sameCount > 1) {
      features :+= makeFea(FeaGroup.same, "1")
      features :+= makeFea(FeaGroup.same_cnt, sameCount.toString)
      features :+= makeFea(FeaGroup.same_before, idx.toString)
      if (idx == 0) {
        features :+= makeFea(FeaGroup.same_first, "1")
      }
      else {
        features :+= makeFea(FeaGroup.same_first, "0")
      }
      if (idx == sameCount - 1) {
        features :+= makeFea(FeaGroup.same_last, "1")
      }
      else {
        features :+= makeFea(FeaGroup.same_last, "0")
      }
      if (idx > 0 && idx < sameCount - 1) {
        features :+= makeFea(FeaGroup.same_mid, "1")
      }
      else {
        features :+= makeFea(FeaGroup.same_mid, "0")
      }
    }
    else {
      features :+= makeFea(FeaGroup.same, "0")
    }
    features
  }

  private def extractCounterFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    if (instance.isSetCounter) {
      val cur = instance.getCounter
      if (cur.getAdvertiser_id != null) {
        features :+= makeFea(FeaGroup.advertiser_id_cvr, getCVR(cur.getAdvertiser_id))
        features :+= makeFea(FeaGroup.advertiser_id_cvr_count, getLabel(cur.getAdvertiser_id))
      }
      else {
        features :+= makeFea(FeaGroup.advertiser_id_first, "1")
      }
      if (cur.getCampaign_id != null) {
        features :+= makeFea(FeaGroup.campaign_id_cvr, getCVR(cur.getCampaign_id))
        features :+= makeFea(FeaGroup.campaign_id_cvr_count, getLabel(cur.getCampaign_id))
      }
      else {
        features :+= makeFea(FeaGroup.campaign_id_first, "1")
      }
      if (cur.getAd_id != null) {
        features :+= makeFea(FeaGroup.ad_id_cvr, getCVR(cur.getAd_id))
        features :+= makeFea(FeaGroup.ad_id_cvr_count, getLabel(cur.getAd_id))
      }
      else {
        features :+= makeFea(FeaGroup.ad_id_first, "1")
      }
      if (cur.getApp_id != null) {
        features :+= makeFea(FeaGroup.app_id_cvr, getCVR(cur.getApp_id))
        features :+= makeFea(FeaGroup.app_id_cvr_count, getLabel(cur.getApp_id))
      }
      else {
        features :+= makeFea(FeaGroup.app_id_first, "1")
      }
      if (cur.getApp_category1 != null) {
        features :+= makeFea(FeaGroup.app_category1_cvr, getCVR(cur.getApp_category1))
        features :+= makeFea(FeaGroup.app_category1_cvr_count, getLabel(cur.getApp_category1))
      }
      else {
        features :+= makeFea(FeaGroup.app_category1_first, "1")
      }
      if (cur.getApp_category2 != null) {
        features :+= makeFea(FeaGroup.app_category2_cvr, getCVR(cur.getApp_category2))
        features :+= makeFea(FeaGroup.app_category2_cvr_count, getLabel(cur.getApp_category2))
      }
      else {
        features :+= makeFea(FeaGroup.app_category2_first, "1")
      }
    }
    if (instance.isSetCounter) {
      val cur = instance.getCounter
      if (cur.getUser_id != null) {
        features :+= makeFea(FeaGroup.user_id_cvr, getCVR(cur.getUser_id))
        features :+= makeFea(FeaGroup.user_id_cvr_count, getLabel(cur.getUser_id))
      }
      else {
        features :+= makeFea(FeaGroup.user_id_cvr_count, "0")
      }
      if (cur.getIp != null) {
        features :+= makeFea(FeaGroup.ip_cvr, getCVR(cur.getIp))
        features :+= makeFea(FeaGroup.ip_cvr_count, getLabel(cur.getIp))
      }
      else {
        features :+= makeFea(FeaGroup.ip_cvr_count, "0")
      }
    }
    features
  }

  private def extractFeatures(instance: CVRInstance, hash: DatasetHelper): List[BaseFea] = {
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
    val app_installed_list = profile.getApp_installed_list

    val app_category_dict = hash.getAppCategory

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

    // features :+= makeFea(FeaGroup.user_id, user_id)
    // features :+= makeFea(FeaGroup.ip, ip)

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

    features :+= makeFea(FeaGroup.miui_version, miui_version)
    features :+= makeFea(FeaGroup.miui_version_advertiser_id, miui_version + cat + advertiser_id)
    features :+= makeFea(FeaGroup.miui_version_campaign_id, miui_version + cat + campaign_id)
    features :+= makeFea(FeaGroup.miui_version_ad_id, miui_version + cat + ad_id)
    features :+= makeFea(FeaGroup.miui_version_app_id, miui_version + cat + app_id)
    features :+= makeFea(FeaGroup.miui_version_app_category1, miui_version + cat + app_category1)
    features :+= makeFea(FeaGroup.miui_version_app_category2, miui_version + cat + app_category2)

    features :+= makeFea(FeaGroup.android_version, android_version)
    features :+= makeFea(FeaGroup.android_version_advertiser_id, android_version + cat + advertiser_id)
    features :+= makeFea(FeaGroup.android_version_campaign_id, android_version + cat + campaign_id)
    features :+= makeFea(FeaGroup.android_version_ad_id, android_version + cat + ad_id)
    features :+= makeFea(FeaGroup.android_version_app_id, android_version + cat + app_id)
    features :+= makeFea(FeaGroup.android_version_app_category1, android_version + cat + app_category1)
    features :+= makeFea(FeaGroup.android_version_app_category2, android_version + cat + app_category2)

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

    features :+= makeFea(FeaGroup.advertiser_id, advertiser_id)
    features :+= makeFea(FeaGroup.campaign_id, campaign_id)
    features :+= makeFea(FeaGroup.ad_id, ad_id)
    features :+= makeFea(FeaGroup.app_id, app_id)
    features :+= makeFea(FeaGroup.app_category1, app_category1)
    features :+= makeFea(FeaGroup.app_category2, app_category2)

    for (tags <- app_description) {
      features :+= makeFea(FeaGroup.app_description, tags.toString)

      features :+= makeFea(FeaGroup.clk_day_app_description, clk_day + cat + tags)
      features :+= makeFea(FeaGroup.clk_hour_app_description, clk_hour + cat + tags)
      features :+= makeFea(FeaGroup.pos_app_description, pos + cat + tags)

      features :+= makeFea(FeaGroup.age_app_description, age + cat + tags)
      features :+= makeFea(FeaGroup.gender_app_description, gender + cat + tags)
      features :+= makeFea(FeaGroup.education_app_description, education + cat + tags)
      features :+= makeFea(FeaGroup.connection_type_app_description, connection_type + cat + tags)

      features :+= makeFea(FeaGroup.province_app_description, province + cat + tags)
      features :+= makeFea(FeaGroup.city_app_description, city + cat + tags)
      features :+= makeFea(FeaGroup.device_info_app_description, device_info + cat + tags)

      features :+= makeFea(FeaGroup.miui_version_app_description, miui_version + cat + tags)
      features :+= makeFea(FeaGroup.android_version_app_description, android_version + cat + tags)
    }

    if (profile.isSetApp_installed_list) {
      val app_installed = profile.getApp_installed_list.map(it => app_category_dict(it))
      val app_count = math.min(app_installed.size / 20, 20)
      features :+= makeFea(FeaGroup.app_installed_count, app_count)
      for (app <- app_installed) {
        features :+= makeFea(FeaGroup.app_installed, app.getApp_id.toString)
      }

      val app_installed_category1 = app_installed.map(_.getApp_category1).groupBy(identity).mapValues(_.size)
      for (app <- app_installed_category1) {
        val cnt = app._2 / 10
        val ratio = 100 * app._2 / app_installed.size
        features :+= makeFea(FeaGroup.app_installed_category1, app._1.toString, cnt)
        features :+= makeFea(FeaGroup.app_installed_category1_ratio, app._1.toString, ratio)
      }

      val app_installed_category2 = app_installed.map(_.getApp_category2).groupBy(identity).mapValues(_.size)
      for (app <- app_installed_category2) {
        val cnt = app._2 / 10
        val ratio = 100 * app._2 / app_installed.size
        features :+= makeFea(FeaGroup.app_installed_category2, app._1.toString, cnt)
        features :+= makeFea(FeaGroup.app_installed_category2_ratio, app._1.toString, ratio)
      }

      val app_installed_match = app_installed.filter(it => it.getApp_id == app_id.toInt)
      features :+= makeFea(FeaGroup.app_installed_match_count, app_installed_match.size)
      for (app <- app_installed_match) {
        features :+= makeFea(FeaGroup.app_installed_match, app.getApp_id.toString)
      }

      val app_installed_category1_match = app_installed_category1.filter(it => it._1 == app_category1.toInt)
      for (app <- app_installed_category1_match) {
        val cnt = app._2 / 10
        val ratio = 100 * app._2 / app_installed.size
        features :+= makeFea(FeaGroup.app_installed_category1_match, app._1.toString, cnt)
        features :+= makeFea(FeaGroup.app_installed_category1_match_ratio, app._1.toString, ratio)
      }

      val app_installed_category2_match = app_installed_category2.filter(it => it._1 == app_category2.toInt)
      for (app <- app_installed_category2_match) {
        val cnt = app._2 / 10
        val ratio = 100 * app._2 / app_installed.size
        features :+= makeFea(FeaGroup.app_installed_category2_match, app._1.toString, cnt)
        features :+= makeFea(FeaGroup.app_installed_category2_match_ratio, app._1.toString, ratio)
      }
    }

    val history = instance.getHistory
    if (history.isSetApp_usage) {
      val days = history.getApp_usage.size()
      // 使用次数最多的app 使用时长最多的app 需要按app_id聚合
      val app_usage = history.getApp_usage.flatMap(_.getApp_usage_list).map(it => (it.getApp_id, (it.getDuration, it.getCount))).groupBy(_._1).toArray.map {
        it => {
          val tmp = it._2.map(_._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
          (it._1, tmp)
        }
      }

      for (app <- app_usage) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_avg_duration, app._1.toString, avg_open_duration)
      }

      val app_usage_category = app_usage.map(it => (app_category_dict.get(it._1), it._2))

      val app_usage_category1 = app_usage_category.map(it => (it._1.getApp_category1, it._2)).groupBy(_._1).map {
        it => {
          val tmp = it._2.map(_._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
          (it._1, tmp)
        }
      }

      for (app <- app_usage_category1) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_category1_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_category1_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_category1_avg_duration, app._1.toString, avg_open_duration)
      }

      val app_usage_category2 = app_usage_category.map(it => (it._1.getApp_category2, it._2)).groupBy(_._1).map {
        it => {
          val tmp = it._2.map(_._2).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
          (it._1, tmp)
        }
      }

      for (app <- app_usage_category2) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_category2_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_category2_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_category2_avg_duration, app._1.toString, avg_open_duration)
      }

      val app_usage_match = app_usage.filter(it => it._1 == app_id.toInt)
      for (app <- app_usage_match) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_match, app._1.toString)
        features :+= makeFea(FeaGroup.app_usage_match_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_match_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_match_avg_duration, app._1.toString, avg_open_duration)
      }

      val app_usage_category1_match = app_usage_category1.filter(it => it._1 == app_category1.toInt)
      for (app <- app_usage_category1_match) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_category1_match, app._1.toString)
        features :+= makeFea(FeaGroup.app_usage_category1_match_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_category1_match_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_category1_match_avg_duration, app._1.toString, avg_open_duration)
      }

      val app_usage_category2_match = app_usage_category2.filter(it => it._1 == app_category2.toInt)
      for (app <- app_usage_category2_match) {
        val avg_day_count = app._2._2 * 1.0 / days
        val avg_day_duration = app._2._1 * 1.0 / 60.0 / days
        val avg_open_duration = app._2._1 * 1.0 / app._2._2 / 60.0
        features :+= makeFea(FeaGroup.app_usage_category2_match, app._1.toString)
        features :+= makeFea(FeaGroup.app_usage_category2_match_count, app._1.toString, avg_day_count)
        features :+= makeFea(FeaGroup.app_usage_category2_match_duration, app._1.toString, avg_day_duration)
        features :+= makeFea(FeaGroup.app_usage_category2_match_avg_duration, app._1.toString, avg_open_duration)
      }
    }

    if (history.isSetApp_actions) {
      val days = history.getApp_actions.size()
      val app_actions_list = history.getApp_actions.flatMap(_.getApp_actions_list).map(it => (app_category_dict.get(it.getApp_id), it.getAction_type))

      val app_actions = app_actions_list.groupBy(it => (it._1.getApp_id, it._2)).mapValues(_.size)
      for (app <- app_actions) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions, fea, avg_count)
      }

      val app_actions_category1 = app_actions_list.groupBy(it => (it._1.getApp_category1, it._2)).mapValues(_.size)
      for (app <- app_actions_category1) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions_category1, fea, avg_count)
      }

      val app_actions_category2 = app_actions_list.groupBy(it => (it._1.getApp_category2, it._2)).mapValues(_.size)
      for (app <- app_actions_category2) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions_category2, fea, avg_count)
      }

      val app_actions_match = app_actions.filter(_._1._1 == app_id.toInt)
      for (app <- app_actions_match) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions_match, fea, avg_count)
      }

      val app_actions_category1_match = app_actions_category1.filter(_._1._1 == app_category1.toInt)
      for (app <- app_actions_category1_match) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions_category1_match, fea, avg_count)
      }

      val app_actions_category2_match = app_actions_category2.filter(_._1._1 == app_category2.toInt)
      for (app <- app_actions_category2_match) {
        val fea = app._1._1 + cat + app._1._2
        val avg_count = app._2 * 1.0 / days
        features :+= makeFea(FeaGroup.app_actions_category2_match, fea, avg_count)
      }
    }

    if (history.isSetNews_feed) {
      val news_feed = history.getNews_feed.flatMap(_.getNews_feed_list)
      val news_tags_count = news_feed.flatMap(_.getTags).toArray.groupBy(identity).mapValues(_.length).toArray.sortWith(_._1 > _._1).take(20)
      for (tags <- news_tags_count) {
        // features :+= makeFea(FeaGroup.news_tags, tags._1.toString)
        features :+= makeFea(FeaGroup.news_tags_count, tags._1.toString, tags._2)
      }
    }

    if (history.isSetQuery) {
      val query = history.getQuery.flatMap(_.getQuery_list)
      val query_tags_count = query.flatMap(_.getTags).toArray.groupBy(identity).mapValues(_.length).toArray.sortWith(_._1 > _._1).take(20)
      for (tags <- query_tags_count) {
        // features :+= makeFea(FeaGroup.query_tags, tags._1.toString)
        features :+= makeFea(FeaGroup.query_tags_count, tags._1.toString, tags._2)
      }
    }

    if (history.isSetShopping) {
      val shopping = history.getShopping.flatMap(_.getShopping_list)
      val shopping_tags_count = shopping.flatMap(_.getTags).toArray.groupBy(identity).mapValues(_.length).toArray.sortWith(_._1 > _._1).take(20)
      for (tags <- shopping_tags_count) {
        // features :+= makeFea(FeaGroup.shopping_tags, tags._1.toString)
        features :+= makeFea(FeaGroup.shopping_tags_count, tags._1.toString, tags._2)
      }
    }
    features
  }
}
