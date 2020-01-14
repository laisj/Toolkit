package com.xiaomi.contest.cvr.process

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features.ExtractorBase
import com.xiaomi.contest.cvr.utils.DataUtils._
import com.xiaomi.contest.cvr.utils.Paths
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object DataStats {
  def extractFeaStats(instance: CVRInstance, stats: Map[String, (Int, Int)]): FeaStats = {
    val baseFeatures = ExtractorBase.extractFeatures(instance)
    val ans = new FeaStats()
    for (fea <- baseFeatures) {
      if (stats.contains(fea.getFea)) {
        ans.putToStats(fea.getGroup_name, makeCounter(stats(fea.getFea))) // 一个group只能有一个feature
      }
    }
    ans
  }

  def calcFeaStats(sc: SparkContext, data: RDD[CVRInstance]): RDD[(String, (Int, Int))] = {
    data.flatMap {
      it => {
        for (fea <- ExtractorBase.extractFeatures(it)) yield {
          (fea.getFea, (1, it.getData.getLabel))
        }
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2), 100)
  }

  def calcUserStats(sc: SparkContext, data: RDD[CVRInstance]): RDD[(Int, UserStats)] = {
    val stats = data.flatMap {
      it => {
        var ans = Array[(String, (Int, Int))]()
        ans :+= (it.getData.getUser_id + "#user_id#" + it.getData.getUser_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#advertiser_id#" + it.getAd.getAdvertiser_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#campaign_id#" + it.getAd.getCampaign_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#ad_id#" + it.getAd.getAd_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#app_id#" + it.getAd.getApp_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#app_category1#" + it.getApp_category.getApp_category1, (1, it.getData.getLabel))
        ans :+= (it.getData.getUser_id + "#app_category2#" + it.getApp_category.getApp_category2, (1, it.getData.getLabel))
        ans
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2), 100).map {
      value => {
        val item = value._1.split('#')
        (item(0).toInt, (item(1), item(2).toInt, value._2))
      }
    }.groupByKey(100).map {
      value => {
        val ans = new UserStats()
        for (it <- value._2) {
          val cnt = makeCounter(it._3)
          if (it._1 == "user_id") {
            ans.setUser_id(it._2)
            ans.setTotal(cnt)
          }
          if (it._1 == "advertiser_id") ans.putToAdvertiser_id(it._2, cnt)
          if (it._1 == "campaign_id") ans.putToCampaign_id(it._2, cnt)
          if (it._1 == "ad_id") ans.putToAd_id(it._2, cnt)
          if (it._1 == "app_id") ans.putToApp_id(it._2, cnt)
          if (it._1 == "app_category1") ans.putToApp_category1(it._2, cnt)
          if (it._1 == "app_category2") ans.putToApp_category2(it._2, cnt)
        }
        (value._1, ans)
      }
    }
    stats
  }

  def calcIpStats(sc: SparkContext, data: RDD[CVRInstance]): RDD[(Int, IpStats)] = {
    val stats = data.flatMap {
      it => {
        var ans = Array[(String, (Int, Int))]()
        ans :+= (it.getData.getIp + "#ip#" + it.getData.getIp, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#advertiser_id#" + it.getAd.getAdvertiser_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#campaign_id#" + it.getAd.getCampaign_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#ad_id#" + it.getAd.getAd_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#app_id#" + it.getAd.getApp_id, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#app_category1#" + it.getApp_category.getApp_category1, (1, it.getData.getLabel))
        ans :+= (it.getData.getIp + "#app_category2#" + it.getApp_category.getApp_category2, (1, it.getData.getLabel))
        ans
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2), 100).map {
      value => {
        val item = value._1.split('#')
        (item(0).toInt, (item(1), item(2).toInt, value._2))
      }
    }.groupByKey(100).map {
      value => {
        val ans = new IpStats()
        for (it <- value._2) {
          val cnt = makeCounter(it._3)
          if (it._1 == "ip") {
            ans.setIp(it._2)
            ans.setTotal(cnt)
          }
          if (it._1 == "advertiser_id") ans.putToAdvertiser_id(it._2, cnt)
          if (it._1 == "campaign_id") ans.putToCampaign_id(it._2, cnt)
          if (it._1 == "ad_id") ans.putToAd_id(it._2, cnt)
          if (it._1 == "app_id") ans.putToApp_id(it._2, cnt)
          if (it._1 == "app_category1") ans.putToApp_category1(it._2, cnt)
          if (it._1 == "app_category2") ans.putToApp_category2(it._2, cnt)
        }
        (value._1, ans)
      }
    }
    stats
  }

  def calcAdStats(sc: SparkContext, data: RDD[CVRInstance]): RDD[(Int, AdStats)] = {
    val stats = data.flatMap {
      it => {
        var ans = Array[(String, (Int, Int))]()
        ans :+= (it.getAd.getAd_id + "#ad_id#" + it.getAd.getAd_id, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#age#" + it.getProfile.getAge, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#gender#" + it.getProfile.getGender, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#education#" + it.getProfile.getEducation, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#province#" + it.getProfile.getProvince, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#city#" + it.getProfile.getCity, (1, it.getData.getLabel))
        ans :+= (it.getAd.getAd_id + "#device_info#" + it.getProfile.getDevice_info, (1, it.getData.getLabel))
        /*
        if (it.getProfile.isSetApp_installed_list) {
          for (app <- it.getProfile.getApp_installed_list) {
            ans :+= (it.getAd.getAd_id + "#app_installed_list#" + app, (1, it.getData.getLabel))
          }
        }
        */
        ans
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2), 100).map {
      value => {
        val item = value._1.split('#')
        (item(0).toInt, (item(1), item(2), value._2))
      }
    }.groupByKey(100).map {
      value => {
        val ans = new AdStats()
        for (it <- value._2) {
          val cnt = makeCounter(it._3)
          if (it._1 == "ad_id") {
            ans.setAd_id(it._2.toInt)
            ans.setTotal(cnt)
          }
          if (it._1 == "age") ans.putToAge(it._2.toInt, cnt)
          if (it._1 == "gender") ans.putToGender(it._2.toInt, cnt)
          if (it._1 == "education") ans.putToEducation(it._2, cnt)
          if (it._1 == "province") ans.putToProvince(it._2.toInt, cnt)
          if (it._1 == "city") ans.putToCity(it._2.toInt, cnt)
          if (it._1 == "device_info") ans.putToDevice_info(it._2.toInt, cnt)
          if (it._1 == "app_installed_list") ans.putToApp_installed_list(it._2.toInt, cnt)
        }
        (value._1, ans)
      }
    }
    stats
  }

  def calcAppStats(sc: SparkContext, data: RDD[CVRInstance]): RDD[(Int, AppStats)] = {
    val stats = data.flatMap {
      it => {
        var ans = Array[(String, (Int, Int))]()
        ans :+= (it.getAd.getApp_id + "#app_id#" + it.getAd.getApp_id, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#age#" + it.getProfile.getAge, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#gender#" + it.getProfile.getGender, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#education#" + it.getProfile.getEducation, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#province#" + it.getProfile.getProvince, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#city#" + it.getProfile.getCity, (1, it.getData.getLabel))
        ans :+= (it.getAd.getApp_id + "#device_info#" + it.getProfile.getDevice_info, (1, it.getData.getLabel))
        /*
        if (it.getProfile.isSetApp_installed_list) {
          for (app <- it.getProfile.getApp_installed_list) {
            ans :+= (it.getAd.getApp_id + "#app_installed_list#" + app, (1, it.getData.getLabel))
          }
        }
        */
        ans
      }
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2), 100).map {
      value => {
        val item = value._1.split('#')
        (item(0).toInt, (item(1), item(2), value._2))
      }
    }.groupByKey(100).map {
      value => {
        val ans = new AppStats()
        for (it <- value._2) {
          val cnt = makeCounter(it._3)
          if (it._1 == "app_id") {
            ans.setApp_id(it._2.toInt)
            ans.setTotal(cnt)
          }
          if (it._1 == "age") ans.putToAge(it._2.toInt, cnt)
          if (it._1 == "gender") ans.putToGender(it._2.toInt, cnt)
          if (it._1 == "education") ans.putToEducation(it._2, cnt)
          if (it._1 == "province") ans.putToProvince(it._2.toInt, cnt)
          if (it._1 == "city") ans.putToCity(it._2.toInt, cnt)
          if (it._1 == "device_info") ans.putToDevice_info(it._2.toInt, cnt)
          if (it._1 == "app_installed_list") ans.putToApp_installed_list(it._2.toInt, cnt)
        }
        (value._1, ans)
      }
    }
    stats
  }

  def calcDatasetAdStats(sc: SparkContext, data: RDD[CVRInstance], prevData: RDD[CVRInstance], output: String): RDD[DatasetStats] = {
    val adStats = calcAdStats(sc, prevData)
    adStats.saveAsTextFile(output + "/ad_stats")
    val hashAdStats = sc.broadcast(adStats.collect.toMap)
    data.map {
      value => {
        val adStats = hashAdStats.value
        val ans = new DatasetStats()
        ans.setInstance_id(value.getData.getInstance_id)
        if (adStats.contains(value.getAd.getAd_id)) ans.setAd_stats(adStats(value.getAd.getAd_id))
        ans
      }
    }
  }

  def calcDatasetStats(sc: SparkContext, data: RDD[CVRInstance], prevData: RDD[CVRInstance], output: String): RDD[DatasetStats] = {
    val feaStats = calcFeaStats(sc, prevData)
    val userStats = calcUserStats(sc, prevData)
    val ipStats = calcIpStats(sc, prevData)
    val adStats = calcAdStats(sc, prevData)
    val appStats = calcAppStats(sc, prevData)

    feaStats.saveAsTextFile(output + "/fea_stats")
    userStats.saveAsTextFile(output + "/user_stats")
    ipStats.saveAsTextFile(output + "/ip_stats")
    adStats.saveAsTextFile(output + "/ad_stats")
    appStats.saveAsTextFile(output + "/app_stats")

    val instanceUserStats = data.map(it => (it.getData.getUser_id, it.getData.getInstance_id))
      .leftOuterJoin(userStats)
      .map(_._2).filter(_._2.isDefined).map(it => (it._1, it._2.get))
    val instanceIpStats = data.map(it => (it.getData.getIp, it.getData.getInstance_id))
      .leftOuterJoin(ipStats)
      .map(_._2).filter(_._2.isDefined).map(it => (it._1, it._2.get))

    val hashFeaStats = sc.broadcast(feaStats.collect.toMap)
    val hashAdStats = sc.broadcast(adStats.collect.toMap)
    val hashAppStats = sc.broadcast(appStats.collect.toMap)
    data.map {
      value => {
        val feaStats = hashFeaStats.value
        val adStats = hashAdStats.value
        val appStats = hashAppStats.value
        val ans = new DatasetStats()
        ans.setInstance_id(value.getData.getInstance_id)
        val fea = extractFeaStats(value, feaStats)
        ans.setFea_stats(fea)
        if (adStats.contains(value.getAd.getAd_id)) ans.setAd_stats(adStats(value.getAd.getAd_id))
        if (appStats.contains(value.getAd.getApp_id)) ans.setApp_stats(appStats(value.getAd.getApp_id))
        (ans.getInstance_id, ans)
      }
    }.leftOuterJoin(instanceUserStats).map {
      value => {
        val ans = value._2._1
        if (value._2._2.isDefined) ans.setUser_stats(value._2._2.get)
        (ans.getInstance_id, ans)
      }
    }.leftOuterJoin(instanceIpStats).map {
      value => {
        val ans = value._2._1
        if (value._2._2.isDefined) ans.setIp_stats(value._2._2.get)
        ans
      }
    }
  }

  def calcTrainSamples(sc: SparkContext, basePath: String, output: String, outputText: String): Unit = {
    for (day <- 12.to(30)) {
      val data = sc.thriftParquetFile(basePath + "/train" + Paths.dayStr(day), classOf[CVRInstance]).repartition(100)
      val prevData = sc.thriftParquetFile(Paths.multipleDaysPath(basePath + "/train", day - 1, day - 11), classOf[CVRInstance])
      val ans = calcDatasetStats(sc, data, prevData, outputText + "/train_debug" + Paths.dayStr(day))
      ans.saveAsParquetFile(output + "/train" + Paths.dayStr(day))
      ans.saveAsTextFile(outputText + "/train" + Paths.dayStr(day))
    }
  }

  def calcTestSamples(sc: SparkContext, basePath: String, output: String, outputText: String): Unit = {
    val data = sc.thriftParquetFile(basePath + "/test", classOf[CVRInstance]).repartition(100)
    val prevData = sc.thriftParquetFile(Paths.multipleDaysPath(basePath + "/train", 30, 20), classOf[CVRInstance])
    val ans = calcDatasetStats(sc, data, prevData, outputText + "/test_debug")
    ans.saveAsParquetFile(output + "/test")
    ans.saveAsTextFile(outputText + "/test")
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Data Stats")

    val sc = new SparkContext(conf)

    val output = Paths.basePath + "/stats"
    val outputText = Paths.basePath + "/stats_text"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    fs.delete(new Path(outputText), true)

    calcTrainSamples(sc, Paths.basePath + "/base", output, outputText)
    calcTestSamples(sc, Paths.basePath + "/base", output, outputText)

    val trainStats = sc.thriftParquetFile(Paths.multipleDaysPath(output + "/train", 30, 19), classOf[DatasetStats])
    val testStats = sc.thriftParquetFile(output + "/test", classOf[DatasetStats])

    val allStats = trainStats.union(testStats).repartition(200)
    allStats.saveAsParquetFile(output + "/all")

    sc.stop()
    println("Job done!")
  }
}
