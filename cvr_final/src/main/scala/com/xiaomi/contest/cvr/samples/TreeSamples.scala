package com.xiaomi.contest.cvr.samples

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features._
import com.xiaomi.contest.cvr.utils.{DataUtils, Paths, SampleUtils}
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TreeSamples {
  def outputSamples(sc: SparkContext, train: RDD[Sample], validation: RDD[Sample], test: RDD[Sample], output: String): Unit = {
    val statsPath = output + "/stats"
    val trainFeatures = SampleUtils.calcFeatures(sc, train, statsPath + "/train")
    // val validationFeatures = SampleUtils.calcFeatures(sc, validation, statsPath + "/validation")
    val testFeatures = SampleUtils.calcFeatures(sc, test, statsPath + "/test")

    // val trainHash = trainFeatures.toMap
    val trainHash = trainFeatures.filter(it => it._2 >= 100).toMap
    // val validationHash = validationFeatures.toMap

    val features = testFeatures.filter(it => trainHash.contains(it._1))
    val stats = SampleUtils.calcStats(sc, features, statsPath)

    val trainSamples = SampleUtils.encodeSamples(sc, train, stats)
    val validationSamples = SampleUtils.encodeSamples(sc, validation, stats)
    val testSamples = SampleUtils.encodeSamples(sc, test, stats)

    val libSVMSamplesPath = output + "/lib_svm_samples"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesPath + "/train", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesPath + "/validation", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesPath + "/test", with_instance_id = false)

    /*
    val instanceIdPath = output + "/instance_id"
    SampleUtils.outputInstance_id(sc, trainSamples, instanceIdPath + "/train")
    SampleUtils.outputInstance_id(sc, validationSamples, instanceIdPath + "/validation")
    SampleUtils.outputInstance_id(sc, testSamples, instanceIdPath + "/test")
    */

    val libSVMSamplesWithIdPath = output + "/lib_svm_samples_with_id"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesWithIdPath + "/train", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesWithIdPath + "/validation", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesWithIdPath + "/test", with_instance_id = true)
  }

  def calcSamples(sc: SparkContext, data: RDD[CVRInstance], testDupAd: Map[Int, Int], testDupUser: Map[Int, Int], helper: DatasetHelper): RDD[Sample] = {
    val hashDupAd = sc.broadcast(testDupAd)
    val hashDupUser = sc.broadcast(testDupUser)
    val hash = sc.broadcast(helper)
    data.map {
      it => {
        (it.getData.getUser_id + "#" + it.getData.getClick_time + "#" + it.getData.getAd_id, it)
      }
    }.groupByKey().flatMap {
      value => {
        val clicked = value._2.toArray.sortBy(_.getData.getClick_time)
        val sameCnt = clicked.length
        for (clk <- clicked.indices) yield {
          val cur = clicked(clk)
          val dupAd = hashDupAd.value.contains(cur.getAd.getAd_id)
          val dupUser = hashDupUser.value.contains(cur.getData.getUser_id)
          ExtractorDnnStatsFea.singleNoTree(clicked(clk), hash.value, clk, sameCnt, dupAd, dupUser)
        }
      }
    }
  }

  def findDupAd(data: RDD[CVRInstance]): RDD[(Int, Int)] = {
    val ans = data.map {
      it => {
        (it.getData.getUser_id + "#" + it.getData.getAd_id + "#" + it.getData.getClick_time, it)
      }
    }.groupByKey().filter(_._2.size > 1).map {
      it => {
        (it._1.split('#')(1).toInt, it._2.size)
      }
    }.reduceByKey(_ + _)
    ans
  }

  def findDupUser(data: RDD[CVRInstance]): RDD[(Int, Int)] = {
    val ans = data.map {
      it => {
        (it.getData.getUser_id + "#" + it.getData.getAd_id + "#" + it.getData.getClick_time, it)
      }
    }.groupByKey().filter(_._2.size > 1).map {
      it => {
        (it._1.split('#')(0).toInt, it._2.size)
      }
    }.reduceByKey(_ + _)
    ans
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args

    val conf = new SparkConf().setAppName("Calc Tree Samples")

    val sc = new SparkContext(conf)

    // val dataPath = Paths.basePath + "/final_instance_2days_all"
    val dataPath = Paths.basePath + "/final_instance/" + "/2days_all"
    val trainData = sc.thriftParquetFile(dataPath + "/train", classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(dataPath + "/validation", classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val basePath = Paths.basePath + "/base"
    val appCategory = sc.thriftParquetFile(basePath + "/app_category", classOf[AppCategory])
    val helper = DataUtils.calcDatasetHelper(sc, appCategory)

    val testDupAd = findDupAd(testData).collect().toMap
    val testDupUser = findDupUser(testData).collect().toMap

    val trainSamples = calcSamples(sc, trainData, testDupAd, testDupUser, helper)
    val validationSamples = calcSamples(sc, validationData, testDupAd, testDupUser, helper)
    val testSamples = calcSamples(sc, testData, testDupAd, testDupUser, helper)

    val output = Paths.basePath + "/tree_samples_stats"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    outputSamples(sc, trainSamples, validationSamples, testSamples, output)

    sc.stop()
    println("Job done!")
  }
}
