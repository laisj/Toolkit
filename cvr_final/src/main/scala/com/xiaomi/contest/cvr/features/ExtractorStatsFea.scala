package com.xiaomi.contest.cvr.features

import com.xiaomi.contest.cvr.group.FeaGroup
import com.xiaomi.contest.cvr.samples.{BaseFea, Sample}
import com.xiaomi.contest.cvr.utils.FeaUtils._
import com.xiaomi.contest.cvr.{CVRInstance, Counter, DatasetHelper}
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object ExtractorStatsFea {
  final private val cat = "_"

  def single(instance: CVRInstance): Sample = {
    val adAppStatsFeatures = extractAdAppStatsFeatures(instance)
    val userIpStatsFeatures = extractUserIpStatsFeatures(instance)
    val countFeatures = extractCountFeatures(instance)
    val ans = adAppStatsFeatures ++ userIpStatsFeatures ++ countFeatures
    makeSample(ans, instance)
  }

  def extractStatsFea(instance: CVRInstance): List[BaseFea] = {
    val adAppStatsFeatures = extractAdAppStatsFeatures(instance)
    val userIpStatsFeatures = extractUserIpStatsFeatures(instance)
    val countFeatures = extractCountFeatures(instance)
    val ans = adAppStatsFeatures ++ userIpStatsFeatures ++ countFeatures
    ans
  }

  def getCVRInt(cnt: Counter): String = {
    (cnt.getLabel * 100 / cnt.getClk).toString
  }

  def getCVR(cnt: Counter): Double = {
    (1 + cnt.getLabel) * 1.0 / (1 + cnt.getClk)
    // cnt.getLabel * 1.0 / cnt.getClk
  }

  def getLabel(cnt: Counter): Int = {
    if (cnt.getLabel > 0) 1
    else 0
  }

  def getLabelInt(cnt: Counter): Double = {
    1.0 * cnt.getLabel
  }

  def extractAdAppStatsFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    if (instance.isSetData_stats) {
      val stats = instance.getData_stats
      if (stats.isSetAd_stats) {
        val adStats = stats.getAd_stats
        val ad_id = adStats.getAd_id
        val total = adStats.getTotal
        features :+= makeFea(FeaGroup.stats_ad_total_cvr, getCVR(total))
        features :+= makeFea(FeaGroup.stats_ad_total_cvr_count, getLabel(total))
        for (fea <- adStats.getAge) {
          features :+= makeFea(FeaGroup.stats_ad_age_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_age_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- adStats.getGender) {
          features :+= makeFea(FeaGroup.stats_ad_gender_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_gender_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- adStats.getEducation) {
          features :+= makeFea(FeaGroup.stats_ad_education_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_education_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- adStats.getProvince.toArray.sortWith(_._2.getClk > _._2.getClk).take(30)) {
          features :+= makeFea(FeaGroup.stats_ad_province_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_province_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- adStats.getCity.toArray.sortWith(_._2.getClk > _._2.getClk).take(50)) {
          features :+= makeFea(FeaGroup.stats_ad_city_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_city_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        /*
        for (fea <- adStats.getDevice_info.toArray.sortWith(_._2.getClk > _._2.getClk).take(100)) {
          features :+= makeFea(FeaGroup.stats_ad_device_info_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_ad_device_info_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        */
      }
      if (stats.isSetApp_stats) {
        val appStats = stats.getApp_stats
        val app_id = appStats.getApp_id
        val total = appStats.getTotal
        features :+= makeFea(FeaGroup.stats_app_total_cvr, getCVR(total))
        features :+= makeFea(FeaGroup.stats_app_total_cvr_count, getLabel(total))
        for (fea <- appStats.getAge) {
          features :+= makeFea(FeaGroup.stats_app_age_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_age_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- appStats.getGender) {
          features :+= makeFea(FeaGroup.stats_app_gender_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_gender_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- appStats.getEducation) {
          features :+= makeFea(FeaGroup.stats_app_education_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_education_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- appStats.getProvince.toArray.sortWith(_._2.getClk > _._2.getClk).take(30)) {
          features :+= makeFea(FeaGroup.stats_app_province_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_province_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- appStats.getCity.toArray.sortWith(_._2.getClk > _._2.getClk).take(50)) {
          features :+= makeFea(FeaGroup.stats_app_city_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_city_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        /*
        for (fea <- appStats.getDevice_info) {
          features :+= makeFea(FeaGroup.stats_app_device_info_cvr, fea._1.toString, getCVR(fea._2))
          // features :+= makeFea(FeaGroup.stats_app_device_info_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        */
      }
    }
    features
  }

  def extractFeaStatsFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    if (instance.isSetData_stats) {
      val stats = instance.getData_stats
      if (stats.isSetFea_stats) {
        for (fea <- stats.getFea_stats.getStats) {
          features :+= makeFea(FeaGroup.stats_fea_cvr, fea._1, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_fea_cvr_count, fea._1, getLabel(fea._2))
        }
      }
    }
    features
  }

  def extractUserIpStatsFeatures(instance: CVRInstance): List[BaseFea] = {
    var features = List[BaseFea]()
    if (instance.isSetData_stats) {
      val stats = instance.getData_stats
      if (stats.isSetUser_stats) {
        val user_stats = stats.getUser_stats
        val user_id = user_stats.getUser_id
        val total = user_stats.getTotal
        features :+= makeFea(FeaGroup.stats_user_total_cvr, getCVR(total))
        features :+= makeFea(FeaGroup.stats_user_total_cvr_count, getLabel(total))
        for (fea <- user_stats.getAdvertiser_id) {
          features :+= makeFea(FeaGroup.stats_user_advertiser_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_advertiser_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- user_stats.getCampaign_id) {
          features :+= makeFea(FeaGroup.stats_user_campaign_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_campaign_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- user_stats.getAd_id) {
          features :+= makeFea(FeaGroup.stats_user_ad_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_ad_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- user_stats.getApp_id) {
          features :+= makeFea(FeaGroup.stats_user_app_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_app_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- user_stats.getApp_category1) {
          features :+= makeFea(FeaGroup.stats_user_app_category1_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_app_category1_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- user_stats.getApp_category2) {
          features :+= makeFea(FeaGroup.stats_user_app_category2_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_user_app_category2_cvr_count, fea._1.toString, getLabel(fea._2))
        }
      }
      if (stats.isSetIp_stats) {
        val ip_stats = stats.getIp_stats
        val ip = ip_stats.getIp
        val total = ip_stats.getTotal
        features :+= makeFea(FeaGroup.stats_ip_total_cvr, getCVR(total))
        features :+= makeFea(FeaGroup.stats_ip_total_cvr_count, getLabel(total))
        for (fea <- ip_stats.getAdvertiser_id) {
          features :+= makeFea(FeaGroup.stats_ip_advertiser_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_advertiser_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- ip_stats.getCampaign_id) {
          features :+= makeFea(FeaGroup.stats_ip_campaign_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_campaign_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- ip_stats.getAd_id) {
          features :+= makeFea(FeaGroup.stats_ip_ad_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_ad_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- ip_stats.getApp_id) {
          features :+= makeFea(FeaGroup.stats_ip_app_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_app_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- ip_stats.getApp_category1) {
          features :+= makeFea(FeaGroup.stats_ip_app_category1_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_app_category1_cvr_count, fea._1.toString, getLabel(fea._2))
        }
        for (fea <- ip_stats.getApp_category2) {
          features :+= makeFea(FeaGroup.stats_ip_app_category2_cvr, fea._1.toString, getCVR(fea._2))
          features :+= makeFea(FeaGroup.stats_ip_app_category2_cvr_count, fea._1.toString, getLabel(fea._2))
        }
      }
    }
    features
  }

  def extractCountFeatures(instance: CVRInstance): List[BaseFea] = {
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
        features :+= makeFea(FeaGroup.user_id_cvr_count, 0)
      }
      if (cur.getIp != null) {
        features :+= makeFea(FeaGroup.ip_cvr, getCVR(cur.getIp))
        features :+= makeFea(FeaGroup.ip_cvr_count, getLabel(cur.getIp))
      }
      else {
        features :+= makeFea(FeaGroup.ip_cvr_count, 0)
      }
    }
    features
  }
}
