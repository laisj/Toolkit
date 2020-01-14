package com.xiaomi.contest.cvr.process

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.utils.Paths
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object DataBehavior {
  def processAppUsage(sc: SparkContext, day: Int): RDD[UserAppUsage] = {
    val data = sc.textFile(Paths.appUsagePath + Paths.dayStr(day)).map {
      value => {
        val item = value.split('\t')
        var appUsageList = List[AppUsage]()
        for (app <- item(1).split('|')) {
          val tmp = app.split(',')
          val usage = new AppUsage()
          usage.setApp_id(tmp(0).toInt)
          usage.setDuration(tmp(1).toInt)
          usage.setCount(tmp(2).toInt)
          usage.setTime("%02d".format(day))
          appUsageList :+= usage
        }
        (item(0).toInt, appUsageList)
      }
    }.groupByKey(100).map {
      value => {
        val usageList = value._2.reduce((a, b) => a ++ b)
        val userAppUsage = new UserAppUsage()
        userAppUsage.setUser_id(value._1)
        userAppUsage.setApp_usage_list(usageList)
      }
    }
    data
  }

  def processAppActions(sc: SparkContext, day: Int): RDD[UserAppActions] = {
    val data = sc.textFile(Paths.appActionsPath + Paths.dayStr(day)).map {
      value => {
        val item = value.split('\t')
        val ans = new AppActions()
        ans.setApp_id(item(1).toInt)
        ans.setAction_type(item(2))
        ans.setTime(item(3))
        (item(0).toInt, ans)
      }
    }.groupByKey(100).map {
      value => {
        val ans = new UserAppActions()
        ans.setUser_id(value._1)
        ans.setApp_actions_list(value._2.toList)
        ans
      }
    }
    data
  }

  def processNewsFeed(sc: SparkContext, day: Int): RDD[UserNewsfeed] = {
    val data = sc.textFile(Paths.newsFeedPath + Paths.dayStr(day)).map {
      value => {
        val item = value.split('\t')
        val ans = new Newsfeed()
        val tags = item(1).split(',').toList
        tags.foreach(it => ans.addToTags(it.toInt))
        ans.setTime(item(2))
        (item(0).toInt, ans)
      }
    }.groupByKey(100).map {
      value => {
        val ans = new UserNewsfeed()
        ans.setUser_id(value._1)
        ans.setNews_feed_list(value._2.toList)
        ans
      }
    }
    data
  }

  def processQuery(sc: SparkContext, day: Int): RDD[UserQuery] = {
    val data = sc.textFile(Paths.queryPath + Paths.dayStr(day)).map {
      value => {
        val item = value.split('\t')
        val ans = new Query()
        val tags = item(1).split(',').toList
        tags.foreach(it => ans.addToTags(it.toInt))
        ans.setTime(item(2))
        (item(0).toInt, ans)
      }
    }.groupByKey(100).map {
      value => {
        val ans = new UserQuery()
        ans.setUser_id(value._1)
        ans.setQuery_list(value._2.toList)
        ans
      }
    }
    data
  }

  def processShopping(sc: SparkContext, day: Int): RDD[UserShopping] = {
    val data = sc.textFile(Paths.shoppingPath + Paths.dayStr(day)).map {
      value => {
        val item = value.split('\t')
        val ans = new Shopping()
        val tags = item(1).split(',').toList
        tags.foreach(it => ans.addToTags(it.toInt))
        ans.setTime(item(2))
        (item(0).toInt, ans)
      }
    }.groupByKey(100).map {
      value => {
        val ans = new UserShopping()
        ans.setUser_id(value._1)
        ans.setShopping_list(value._2.toList)
        ans
      }
    }
    data
  }

  def processUserBehavior(sc: SparkContext, output: String, outputText: String): Unit = {
    for (day <- 1.to(30)) {
      val appUsage = processAppUsage(sc, day)
      appUsage.cache()
      appUsage.saveAsParquetFile(output + "/app_usage" + Paths.dayStr(day))
      appUsage.saveAsTextFile(outputText + "/app_usage" + Paths.dayStr(day))
    }
    for (day <- 1.to(30)) {
      val appActions = processAppActions(sc, day)
      appActions.cache()
      appActions.saveAsParquetFile(output + "/app_actions" + Paths.dayStr(day))
      appActions.saveAsTextFile(outputText + "/app_actions" + Paths.dayStr(day))
    }
    for (day <- 1.to(30)) {
      val newsFeed = processNewsFeed(sc, day)
      newsFeed.cache()
      newsFeed.saveAsParquetFile(output + "/news_feed" + Paths.dayStr(day))
      newsFeed.saveAsTextFile(outputText + "/news_feed" + Paths.dayStr(day))
    }
    for (day <- 1.to(30)) {
      val query = processQuery(sc, day)
      query.cache()
      query.saveAsParquetFile(output + "/query" + Paths.dayStr(day))
      query.saveAsTextFile(outputText + "/query" + Paths.dayStr(day))
    }
    for (day <- 1.to(30)) {
      val shopping = processShopping(sc, day)
      shopping.cache()
      shopping.saveAsParquetFile(output + "/shopping" + Paths.dayStr(day))
      shopping.saveAsTextFile(outputText + "/shopping" + Paths.dayStr(day))
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Data Process Behavior")

    val sc = new SparkContext(conf)

    val output = Paths.basePath + "/behavior"
    val outputText = Paths.basePath + "/behavior_text"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    fs.delete(new Path(outputText), true)

    processUserBehavior(sc, output, outputText)

    sc.stop()
    println("Job done!")
  }
}
