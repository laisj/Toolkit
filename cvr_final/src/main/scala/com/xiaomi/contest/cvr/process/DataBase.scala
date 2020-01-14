package com.xiaomi.contest.cvr.process

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.utils.{DataUtils, Paths}
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataBase {
  def processAd(sc: SparkContext): RDD[AdInfo] = {
    sc.textFile(Paths.adPath).map {
      value => {
        val item = value.split('\t')
        val ans = new AdInfo()
        ans.setAdvertiser_id(item(0).toInt)
        ans.setCampaign_id(item(1).toInt)
        ans.setAd_id(item(2).toInt)
        ans.setApp_id(item(3).toInt)
      }
    }
  }

  def processAppCategory(sc: SparkContext): RDD[AppCategory] = {
    sc.textFile(Paths.appCategoryPath).map {
      value => {
        val item = value.split('\t')
        val ans = new AppCategory()
        ans.setApp_id(item(0).toInt)
        if (item(1).nonEmpty) {
          val desc_tags = item(1).split(',')
          desc_tags.foreach(it => ans.addToApp_description(it.toInt))
        }
        ans.setApp_category1(item(2).toInt)
        ans.setApp_category2(item(3).toInt)
        ans
      }
    }
  }

  def processUserProfile(sc: SparkContext): RDD[UserProfile] = {
    sc.textFile(Paths.userProfilePath).map {
      value => {
        val item = value.split('\t')
        val ans = new UserProfile()
        ans.setUser_id(item(0).toInt)
        ans.setAge(item(1).toInt)
        ans.setGender(item(2).toInt)
        ans.setEducation(item(3))
        ans.setProvince(item(4).toInt)
        ans.setCity(item(5).toInt)
        ans.setDevice_info(item(6).toInt)
        val app_list = {
          if (item(7) != "\\N") item(7).split(',').toList
          else List[String]()
        }
        app_list.foreach(it => ans.addToApp_installed_list(it.toInt))
        ans
      }
    }
  }

  def getInstance(value: String): Label = {
    val item = value.split('\t')
    val ans = new Label()
    ans.setInstance_id(item(0).toInt)
    ans.setLabel(item(1).toInt)
    ans.setClick_time(item(2))
    ans.setAd_id(item(3).toInt)
    ans.setUser_id(item(4).toInt)
    ans.setPosition_id(item(5).toInt)
    ans.setConnection_type(item(6))
    ans.setMiui_version(item(7).toInt)
    ans.setIp(item(8).toInt)
    ans.setAndroid_version(item(9).toInt)
    ans
  }

  def mergeDataset(sc: SparkContext, data: RDD[Label], ad: RDD[AdInfo], appCategory: RDD[AppCategory], userProfile: RDD[UserProfile]): RDD[CVRInstance] = {
    val adHash = sc.broadcast(ad.map(it => (it.getAd_id, it)).collectAsMap())
    val appCategoryHash = sc.broadcast(appCategory.map(it => (it.getApp_id, it)).collectAsMap())
    val tt = userProfile.map(tt => (tt.getUser_id, tt))
    val instance = data.map(it => (it.getUser_id, it)).leftOuterJoin(tt).map {
      value => {
        val ans = new CVRInstance()
        ans.setData(value._2._1)
        if (value._2._2.isDefined) ans.setProfile(value._2._2.get)
        val adId = value._2._1.getAd_id
        if (adHash.value.contains(adId)) {
          ans.setAd(adHash.value(adId))
          val appId = adHash.value(adId).getApp_id
          if (appCategoryHash.value.contains(appId)) {
            ans.setApp_category(appCategoryHash.value(appId))
          }
        }
        ans
      }
    }
    instance
  }

  def processTrainDataset(sc: SparkContext, ad: RDD[AdInfo], appCategory: RDD[AppCategory], userProfile: RDD[UserProfile], output: String, outputText: String): Unit = {
    for (day <- 11.to(30)) {
      val data = sc.textFile(Paths.trainDatasetPath + Paths.dayStr(day)).map(value => getInstance(value))
      val curData = mergeDataset(sc, data, ad, appCategory, userProfile) // merge ad appCategory userProfile
      val ans = {
        if (day == 11) curData
        else {
          val prevData = sc.thriftParquetFile(Paths.multipleDaysPath(output + "/train", day - 1, day - 11), classOf[CVRInstance])
          val counter = DataUtils.calcDatasetCounter(sc, prevData)
          val hash = sc.broadcast(counter)
          curData.map {
            it => {
              val currentCounter = DataUtils.calcCurrentCounter(it, hash.value)
              it.setCounter(currentCounter)
              it
            }
          }
        }
      }
      ans.saveAsParquetFile(output + "/train" + Paths.dayStr(day))
      ans.saveAsTextFile(outputText + "/train" + Paths.dayStr(day))
    }
  }

  def processTestDataset(sc: SparkContext, ad: RDD[AdInfo], appCategory: RDD[AppCategory], userProfile: RDD[UserProfile], output: String, outputText: String): Unit = {
    val data = sc.textFile(Paths.testDatasetPath).map(value => getInstance(value))
    val curData = mergeDataset(sc, data, ad, appCategory, userProfile)
    val prevData = sc.thriftParquetFile(Paths.multipleDaysPath(output + "/train", 30, 20), classOf[CVRInstance])
    val counter = DataUtils.calcDatasetCounter(sc, prevData)
    val hash = sc.broadcast(counter)
    val ans = curData.map {
      it => {
        val currentCounter = DataUtils.calcCurrentCounter(it, hash.value)
        it.setCounter(currentCounter)
        it
      }
    }
    ans.saveAsParquetFile(output + "/test")
    ans.saveAsTextFile(outputText + "/test")
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Data Base")

    val sc = new SparkContext(conf)

    val output = Paths.basePath + "/base"
    val outputText = Paths.basePath + "/base_text"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    fs.delete(new Path(outputText), true)

    val ad = processAd(sc)
    val appCategory = processAppCategory(sc)
    val userProfile = processUserProfile(sc)

    ad.saveAsParquetFile(output + "/ad")
    appCategory.saveAsParquetFile(output + "/app_category")
    userProfile.saveAsParquetFile(output + "/user_profile")

    ad.saveAsTextFile(outputText + "/ad")
    appCategory.saveAsTextFile(outputText + "/app_category")
    userProfile.saveAsTextFile(outputText + "/user_profile")

    processTrainDataset(sc, ad, appCategory, userProfile, output, outputText)
    processTestDataset(sc, ad, appCategory, userProfile, output, outputText)

    sc.stop()
    println("Job done!")
  }
}
