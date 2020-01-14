package com.xiaomi.contest.cvr.process

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.utils.Paths
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object DataGenerator {
  def processInstance(sc: SparkContext, data: RDD[CVRInstance], behaviorPath: String, end: Int, days: Int): RDD[CVRInstance] = {
    val appUsagePath = Paths.multipleDaysPath(behaviorPath + "/app_usage", end, days)
    val appUsage = sc.thriftParquetFile(appUsagePath, classOf[UserAppUsage])
        .map(it => (it.getUser_id, it)).groupByKey(200)

    val appActionsPath = Paths.multipleDaysPath(behaviorPath + "/app_actions", end, days)
    val appActions = sc.thriftParquetFile(appActionsPath, classOf[UserAppActions])
        .map(it => (it.getUser_id, it)).groupByKey(200)

    val newsFeedPath = Paths.multipleDaysPath(behaviorPath + "/news_feed", end, days)
    val newsFeed = sc.thriftParquetFile(newsFeedPath, classOf[UserNewsfeed])
        .map(it => (it.getUser_id, it)).groupByKey(200)

    val queryPath = Paths.multipleDaysPath(behaviorPath + "/query", end, days)
    val query = sc.thriftParquetFile(queryPath, classOf[UserQuery])
        .map(it => (it.getUser_id, it)).groupByKey(200)

    val shoppingPath = Paths.multipleDaysPath(behaviorPath + "/shopping", end, days)
    val shopping = sc.thriftParquetFile(shoppingPath, classOf[UserShopping])
        .map(it => (it.getUser_id, it)).groupByKey(200)

    val base = data.map(_.getData.getUser_id).distinct().map(it => (it, new UserHistory()))
    val history = base.leftOuterJoin(appUsage).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setApp_usage(value._2._2.get.toList)
        (value._1, tmp)
      }
    }.leftOuterJoin(appActions).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setApp_actions(value._2._2.get.toList)
        (value._1, tmp)
      }
    }.leftOuterJoin(newsFeed).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setNews_feed(value._2._2.get.toList)
        (value._1, tmp)
      }
    }.leftOuterJoin(query).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setQuery(value._2._2.get.toList)
        (value._1, tmp)
      }
    }.leftOuterJoin(shopping).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setShopping(value._2._2.get.toList)
        (value._1, tmp)
      }
    }
    val ans = data.map(it => (it.getData.getUser_id, it)).leftOuterJoin(history).map {
      value => {
        val tmp = value._2._1
        if (value._2._2.isDefined) tmp.setHistory(value._2._2.get)
        tmp
      }
    }
    ans
  }

  def processTrainDataset(sc: SparkContext, basePath: String, behaviorPath: String, output: String, outputText: String): Unit = {
    for (day <- 11.to(30)) {
      val data = sc.thriftParquetFile(basePath + "/train" + Paths.dayStr(day), classOf[CVRInstance]).repartition(200)
      // val tmp = processInstance(sc, data, behaviorPath, day - 3, day - 3)
      val tmp = processInstance(sc, data, behaviorPath, day - 2, day - 2)
      // val tmp = processInstance(sc, data, day - 2, 9)
      tmp.saveAsParquetFile(output + Paths.dayStr(day))
      tmp.saveAsTextFile(outputText + Paths.dayStr(day) + "/text")
    }
  }

  def processTestDataset(sc: SparkContext, basePath: String, behaviorPath: String, output: String, outputText: String): Unit = {
    val data = sc.thriftParquetFile(basePath + "/test", classOf[CVRInstance]).repartition(400)
    // val tmp = processInstance(sc, data, behaviorPath, 32 - 3, 32 - 3)
    val tmp = processInstance(sc, data, behaviorPath, 32 - 2, 32 - 2)
    // val tmp = processInstance(sc, data, 32 - 2, 9)
    tmp.saveAsParquetFile(output)
    tmp.saveAsTextFile(outputText + "/text")
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Data Generator")

    val sc = new SparkContext(conf)

    val basePath = Paths.basePath + "/base"
    val behaviorPath = Paths.basePath + "/behavior"

    val output = Paths.basePath + "/data_2days_all"
    val outputText = Paths.basePath + "/data_2days_all_text"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    fs.delete(new Path(outputText), true)

    processTrainDataset(sc, basePath, behaviorPath, output + "/train", outputText + "/train")
    processTestDataset(sc, basePath, behaviorPath, output + "/test", outputText + "/test")

    sc.stop()
    println("Job done!")
  }
}
