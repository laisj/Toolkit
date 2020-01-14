package com.xiaomi.contest.cvr.analysis

import com.xiaomi.contest.cvr.utils.Paths
import com.xiaomi.contest.cvr.CVRInstance
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object DataAnalysis {
  def calcCount(sc: SparkContext): Unit = {
    val dataPath = Paths.basePath + "/data"

    var userIdSet = Set[Int]()
    var advertiserIdSet = Set[Int]()
    var campaignIdSet = Set[Int]()
    var adIdSet = Set[Int]()
    var appIdSet = Set[Int]()

    for (day <- 11.to(30)) {
      val data = sc.thriftParquetFile(dataPath + "/train" + Paths.dayStr(day), classOf[CVRInstance])

      val userId = data.map(it => it.getData.getUser_id).distinct().collect().toSet
      val advertiserId = data.map(it => it.getAd.getAdvertiser_id).distinct().collect().toSet
      val campaignId = data.map(it => it.getAd.getCampaign_id).distinct().collect().toSet
      val adId = data.map(it => it.getAd.getAd_id).distinct().collect().toSet
      val appId = data.map(it => it.getAd.getApp_id).distinct().collect().toSet

      userIdSet ++= userId
      advertiserIdSet ++= advertiserId
      campaignIdSet ++= campaignId
      adIdSet ++= adId
      appIdSet ++= appId

      printf("xxxxx, %02d, cur, user_id_size: %d, advertiser_id_size: %d, campaign_id_size: %d, ad_id_size: %d, app_id_size: %d\n",
        day, userId.size, advertiserId.size, campaignId.size, adId.size, appId.size)
      printf("xxxxx, %02d, user_id_size: %d, advertiser_id_size: %d, campaign_id_size: %d, ad_id_size: %d, app_id_size: %d\n",
        day, userIdSet.size, advertiserIdSet.size, campaignIdSet.size, adIdSet.size, appIdSet.size)
    }
    val data = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance]).repartition(100)

    val userIdTest = data.map(it => it.getData.getUser_id).distinct().collect().toSet
    val advertiserIdTest = data.map(it => it.getAd.getAdvertiser_id).distinct().collect().toSet
    val campaignIdTest = data.map(it => it.getAd.getCampaign_id).distinct().collect().toSet
    val adIdTest = data.map(it => it.getAd.getAd_id).distinct().collect().toSet
    val appIdTest = data.map(it => it.getAd.getApp_id).distinct().collect().toSet

    userIdSet ++= userIdTest
    advertiserIdSet ++= advertiserIdTest
    campaignIdSet ++= campaignIdTest
    adIdSet ++= adIdTest
    appIdSet ++= appIdTest

    printf("xxxxx, test, cur, user_id_size: %d, advertiser_id_size: %d, campaign_id_size: %d, ad_id_size: %d, app_id_size: %d\n",
      userIdTest.size, advertiserIdTest.size, campaignIdTest.size, adIdTest.size, appIdTest.size)
    printf("xxxxx, test, user_id_size: %d, advertiser_id_size: %d, campaign_id_size: %d, ad_id_size: %d, app_id_size: %d\n",
      userIdSet.size, advertiserIdSet.size, campaignIdSet.size, adIdSet.size, appIdSet.size)
  }

  def calcCVR(sc: SparkContext, output: String): Unit = {
    val dataPath = Paths.basePath + "/data"
    for (day <- 12.to(30)) {
      val path = Paths.multipleDaysPath(dataPath + "/train", day - 1, day - 11)
      println("cvr_path: " + path)
      val data = sc.thriftParquetFile(path, classOf[CVRInstance]).repartition(100)

      val position = data.map(it => (it.getData.getPosition_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val app_category1 = data.map(it => (it.getApp_category.getApp_category1, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val app_category2 = data.map(it => (it.getApp_category.getApp_category2, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

      val userId = data.map(it => (it.getData.getUser_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val advertiserId = data.map(it => (it.getAd.getAdvertiser_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val campaignId = data.map(it => (it.getAd.getCampaign_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val adId = data.map(it => (it.getAd.getAd_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      val appId = data.map(it => (it.getAd.getApp_id, (1, it.getData.getLabel))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

      position.saveAsTextFile(output + "/position" + Paths.dayStr(day))
      app_category1.saveAsTextFile(output + "/app_category1" + Paths.dayStr(day))
      app_category2.saveAsTextFile(output + "/app_category2" + Paths.dayStr(day))

      userId.saveAsTextFile(output + "/user_id" + Paths.dayStr(day))
      advertiserId.saveAsTextFile(output + "/advertiser_id" + Paths.dayStr(day))
      campaignId.saveAsTextFile(output + "/campaign_id" + Paths.dayStr(day))
      adId.saveAsTextFile(output + "/ad_id" + Paths.dayStr(day))
      appId.saveAsTextFile(output + "/app_id" + Paths.dayStr(day))
    }
  }

  def calcShow(sc: SparkContext, output: String): Unit = {
    val dataPath = Paths.basePath + "/data"

    var userIdSet = Array[(Int, Int)]()
    var advertiserIdSet = Array[(Int, Int)]()
    var campaignIdSet = Array[(Int, Int)]()
    var adIdSet = Array[(Int, Int)]()
    var appIdSet = Array[(Int, Int)]()

    for (day <- 11.to(30)) {
      val data = sc.thriftParquetFile(dataPath + "/train" + Paths.dayStr(day), classOf[CVRInstance])

      val userId = data.map(it => it.getData.getUser_id).distinct().collect().map(it => (it, day))
      val advertiserId = data.map(it => it.getAd.getAdvertiser_id).distinct().collect().map(it => (it, day))
      val campaignId = data.map(it => it.getAd.getCampaign_id).distinct().collect().map(it => (it, day))
      val adId = data.map(it => it.getAd.getAd_id).distinct().collect().map(it => (it, day))
      val appId = data.map(it => it.getAd.getApp_id).distinct().collect().map(it => (it, day))

      userIdSet ++= userId
      advertiserIdSet ++= advertiserId
      campaignIdSet ++= campaignId
      adIdSet ++= adId
      appIdSet ++= appId
    }
    val data = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val userId1 = data.map(it => it.getData.getUser_id).distinct().collect().map(it => (it, 31))
    val advertiserId1 = data.map(it => it.getAd.getAdvertiser_id).distinct().collect().map(it => (it, 31))
    val campaignId1 = data.map(it => it.getAd.getCampaign_id).distinct().collect().map(it => (it, 31))
    val adId1 = data.map(it => it.getAd.getAd_id).distinct().collect().map(it => (it, 31))
    val appId1 = data.map(it => it.getAd.getApp_id).distinct().collect().map(it => (it, 31))

    userIdSet ++= userId1
    advertiserIdSet ++= advertiserId1
    campaignIdSet ++= campaignId1
    adIdSet ++= adId1
    appIdSet ++= appId1

    val userId = userIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val advertiserId = advertiserIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val campaignId = campaignIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val adId = adIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val appId = appIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray

    sc.parallelize(userId).saveAsTextFile(output + "/user_id")
    sc.parallelize(advertiserId).saveAsTextFile(output + "/advertiser_id")
    sc.parallelize(campaignId).saveAsTextFile(output + "/campaign_id")
    sc.parallelize(adId).saveAsTextFile(output + "/ad_id")
    sc.parallelize(appId).saveAsTextFile(output + "/app_id")
  }

  def calcAllShow(sc: SparkContext, output: String): Unit = {
    val dataPath = Paths.basePath + "/data"

    var userIdSet = Array[(Int, Int)]()
    var advertiserIdSet = Array[(Int, Int)]()
    var campaignIdSet = Array[(Int, Int)]()
    var adIdSet = Array[(Int, Int)]()
    var appIdSet = Array[(Int, Int)]()

    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 20), classOf[CVRInstance]).repartition(900)
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance]).repartition(100)

    for (day <- 11.to(30)) {
      val data = sc.thriftParquetFile(dataPath + "/train" + Paths.dayStr(day), classOf[CVRInstance])

      val userId = data.map(it => it.getData.getUser_id).distinct().collect().map(it => (it, day))
      val advertiserId = data.map(it => it.getAd.getAdvertiser_id).distinct().collect().map(it => (it, day))
      val campaignId = data.map(it => it.getAd.getCampaign_id).distinct().collect().map(it => (it, day))
      val adId = data.map(it => it.getAd.getAd_id).distinct().collect().map(it => (it, day))
      val appId = data.map(it => it.getAd.getApp_id).distinct().collect().map(it => (it, day))

      userIdSet ++= userId
      advertiserIdSet ++= advertiserId
      campaignIdSet ++= campaignId
      adIdSet ++= adId
      appIdSet ++= appId
    }

    val userId = userIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val advertiserId = advertiserIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val campaignId = campaignIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val adId = adIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray
    val appId = appIdSet.groupBy(_._1).map(it => (it._1, it._2.map(_._2).sorted.mkString(" "))).toArray

    sc.parallelize(userId).saveAsTextFile(output + "/user_id")
    sc.parallelize(advertiserId).saveAsTextFile(output + "/advertiser_id")
    sc.parallelize(campaignId).saveAsTextFile(output + "/campaign_id")
    sc.parallelize(adId).saveAsTextFile(output + "/ad_id")
    sc.parallelize(appId).saveAsTextFile(output + "/app_id")
  }

  def timeSamples(sc: SparkContext, output: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)

    val instancePath = Paths.basePath + "/base"

    val trainPath = Paths.multipleDaysPath(instancePath + "/train", 28, 18)
    val validationPath = Paths.multipleDaysPath(instancePath + "/train", 30, 2)
    val testPath = instancePath + "/test"

    val trainInstance = sc.thriftParquetFile(trainPath, classOf[CVRInstance])
    val validationInstance = sc.thriftParquetFile(validationPath, classOf[CVRInstance])
    val testInstance = sc.thriftParquetFile(testPath, classOf[CVRInstance])

    val train = trainInstance.map(it => (it.getData.getUser_id + "\t" + it.getData.getAd_id, it)).groupByKey().flatMapValues {
      value => {
        value.toArray.sortBy(_.getData.getClick_time)
      }
    }.map {
      value => {
        value._1 + "\t" + value._2.getData.getClick_time + "\t" + value._2.toString
      }
    }

    val validation = validationInstance.map(it => (it.getData.getUser_id + "\t" + it.getData.getAd_id, it)).groupByKey().flatMapValues {
      value => {
        value.toArray.sortBy(_.getData.getClick_time)
      }
    }.map {
      value => {
        value._1 + "\t" + value._2.getData.getClick_time + "\t" + value._2.toString
      }
    }

    val test = testInstance.map(it => (it.getData.getUser_id + "\t" + it.getData.getAd_id, it)).groupByKey().flatMapValues {
      value => {
        value.toArray.sortBy(_.getData.getClick_time)
      }
    }.map {
      value => {
        value._1 + "\t" + value._2.getData.getClick_time + "\t" + value._2.toString
      }
    }

    train.saveAsTextFile(output + "/train")
    validation.saveAsTextFile(output + "/validation")
    test.saveAsTextFile(output + "/test")
  }

  def countInstances(data: RDD[CVRInstance], output: String): Unit = {
    val tmp = data.map(it => (it.getData.getUser_id + "#" + it.getAd.getAd_id, it)).groupByKey().mapValues(it => it.toArray.sortBy(_.getData.getClick_time))
    val org = tmp.flatMap {
      value => {
        val length = value._2.length
        for (it <- value._2) yield {
          it.unsetProfile()
          it.unsetApp_category()
          value._1 + "\t" + length + "\t" + it.getData.getLabel + "\t" + it.getData.getClick_time + "\t" + it.toString
        }
      }
    }
    org.saveAsTextFile(output + "/org")
    val ans = tmp.filter(it => it._2.length > 1).flatMap {
      value => {
        val length = value._2.length
        for (it <- value._2) yield {
          it.unsetProfile()
          it.unsetApp_category()
          value._1 + "\t" + length + "\t" + it.getData.getLabel + "\t" + it.getData.getClick_time + "\t" + it.toString
        }
      }
    }
    ans.saveAsTextFile(output + "/dedup")
  }

  def countSamples(sc: SparkContext, output: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)

    val instancePath = Paths.basePath + "/base"

    val trainPath = Paths.multipleDaysPath(instancePath + "/train", 28, 18)
    val validationPath = Paths.multipleDaysPath(instancePath + "/train", 30, 2)
    val testPath = instancePath + "/test"

    val trainInstance = sc.thriftParquetFile(trainPath, classOf[CVRInstance])
    val validationInstance = sc.thriftParquetFile(validationPath, classOf[CVRInstance])
    val testInstance = sc.thriftParquetFile(testPath, classOf[CVRInstance])

    countInstances(trainInstance, output + "/train")
    countInstances(validationInstance, output + "/validation")
    countInstances(testInstance, output + "/test")
  }

  def appInstallCount(sc: SparkContext, output: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)

    val instancePath = Paths.basePath + "/data"

    val trainPath = Paths.multipleDaysPath(instancePath + "/train", 28, 18)
    val validationPath = Paths.multipleDaysPath(instancePath + "/train", 30, 2)
    val testPath = instancePath + "/test"

    val trainInstance = sc.thriftParquetFile(trainPath, classOf[CVRInstance])
    val validationInstance = sc.thriftParquetFile(validationPath, classOf[CVRInstance])
    val testInstance = sc.thriftParquetFile(testPath, classOf[CVRInstance])

    val data = trainInstance.union(validationInstance).union(testInstance).flatMap {
      value => {
        for (app <- value.getProfile.getApp_installed_list) yield {
          (app, 1)
        }
      }
    }
    data.reduceByKey(_ + _).map(it => it._1 + "\t" + it._2).saveAsTextFile(output)

  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Data Analysis")

    val sc = new SparkContext(conf)

    val output = Paths.basePath + "/analysis"

    // appInstallCount(sc, output + "/app_install")
    countSamples(sc, output + "/count")
    // timeSamples(sc, output + "/time")
    // calcShow(sc, output + "/show")
    // calcCVR(sc, output + "/cvr")
    // calcCount(sc)

    sc.stop()
    println("Job done!")
  }
}
