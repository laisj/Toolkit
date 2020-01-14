package com.xiaomi.contest.cvr.utils

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features.ExtractorBase
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

object DataUtils {
  def makeCounter(it: (Int, Int)): Counter = {
    val counter = new Counter()
    counter.setClk(it._1)
    counter.setLabel(it._2)
    counter
  }

  def calcDatasetCounter(sc: SparkContext, data: RDD[CVRInstance]): DatasetCounter = {
    val advertiserId = data.map(it => (it.getAd.getAdvertiser_id, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))
    val campaignId = data.map(it => (it.getAd.getCampaign_id, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))
    val adId = data.map(it => (it.getAd.getAd_id, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))
    val appId = data.map(it => (it.getAd.getApp_id, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))
    val appCategory1 = data.map(it => (it.getApp_category.getApp_category1, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))
    val appCategory2 = data.map(it => (it.getApp_category.getApp_category2, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .map(it => (it._1, makeCounter(it._2)))

    val userId = data.map(it => (it.getData.getUser_id, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .filter(_._2._2 > 0)
        .map(it => (it._1, makeCounter(it._2)))
    val ip = data.map(it => (it.getData.getIp, (1, it.getData.getLabel)))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .filter(_._2._2 > 0)
        .map(it => (it._1, makeCounter(it._2)))

    val count = new DatasetCounter()
    advertiserId.collect().foreach(it => count.putToAdvertiser_id(it._1, it._2))
    campaignId.collect().foreach(it => count.putToCampaign_id(it._1, it._2))
    adId.collect().foreach(it => count.putToAd_id(it._1, it._2))
    appId.collect().foreach(it => count.putToApp_id(it._1, it._2))
    appCategory1.collect().foreach(it => count.putToApp_category1(it._1, it._2))
    appCategory2.collect().foreach(it => count.putToApp_category2(it._1, it._2))

    userId.collect().foreach(it => count.putToUser_id(it._1, it._2))
    ip.collect().foreach(it => count.putToIp(it._1, it._2))
    count
  }

  def calcCurrentCounter(cur: CVRInstance, stats: DatasetCounter): CurrentCounter = {
    val ans = new CurrentCounter()
    ans.setAdvertiser_id(stats.getAdvertiser_id.get(cur.getAd.getAdvertiser_id))
    ans.setCampaign_id(stats.getCampaign_id.get(cur.getAd.getCampaign_id))
    ans.setAd_id(stats.getAd_id.get(cur.getAd.getAd_id))
    ans.setApp_id(stats.getApp_id.get(cur.getAd.getApp_id))
    ans.setApp_category1(stats.getApp_category1.get(cur.getApp_category.getApp_category1))
    ans.setApp_category2(stats.getApp_category2.get(cur.getApp_category.getApp_category2))

    ans.setUser_id(stats.getUser_id.get(cur.getData.getUser_id))
    ans.setIp(stats.getIp.get(cur.getData.getIp))
  }

  def calcDatasetHelper(sc: SparkContext, appCategory: RDD[AppCategory]): DatasetHelper = {
    val helper = new DatasetHelper()
    appCategory.collect().foreach(it => helper.putToAppCategory(it.getApp_id, it))
    helper
  }
}
