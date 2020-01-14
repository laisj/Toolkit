package com.xiaomi.contest.cvr.samples

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features._
import com.xiaomi.contest.cvr.utils.{DataUtils, Paths, SampleUtils}
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WideDeepSamples {
  def outputSamples(sc: SparkContext, train: RDD[Sample], validation: RDD[Sample], test: RDD[Sample], output: String): Unit = {
    val statsPath = output + "/stats"
    val stats = SampleUtils.calcFeaturesWide(sc, train, statsPath + "/train")

    val trainSamples = SampleUtils.transSamplesWide(sc, train, stats)
    val validationSamples = SampleUtils.transSamplesWide(sc, validation, stats)
    val testSamples = SampleUtils.transSamplesWide(sc, test, stats)

    /*
    val samplesPath = output + "/samples"
    trainSamples.sample(withReplacement = false, 0.001, 11L).saveAsTextFile(samplesPath + "/train")
    validationSamples.sample(withReplacement = false, 0.001, 11L).saveAsTextFile(samplesPath + "/validation")
    testSamples.sample(withReplacement = false, 0.001, 11L).saveAsTextFile(samplesPath + "/test")
    */

    val dnnSamplesPath = output + "/dnn_samples"
    SampleUtils.outputDnnWideSplitSamples(sc, trainSamples, dnnSamplesPath + "/train")
    SampleUtils.outputDnnWideSplitSamples(sc, validationSamples, dnnSamplesPath + "/validation")
    SampleUtils.outputDnnWideSplitSamples(sc, testSamples, dnnSamplesPath + "/test")
  }

  def loadTreeFeatures(sc: SparkContext, path: String, num: Int): RDD[TreeFeatures] = {
    sc.textFile(path).map {
      line => {
        val item = line.split(',')
        val tree = new TreeFeatures()
        tree.setInstance_id(item(0).toInt)
        item(1).split('\t').take(num).foreach(it => tree.addToLeaf_indices(it.toInt))
        tree
      }
    }
  }

  def calcSamples(sc: SparkContext, treeFeatures: RDD[TreeFeatures], data: RDD[CVRInstance], testDupAd: Map[Int, Int], testDupUser: Map[Int, Int], helper: DatasetHelper): RDD[Sample] = {
    val hashDupAd = sc.broadcast(testDupAd)
    val hashDupUser = sc.broadcast(testDupUser)
    val hash = sc.broadcast(helper)
    val tmp = treeFeatures.map(it => (it.getInstance_id, it))
    data.map(it => (it.getData.getInstance_id, it)).leftOuterJoin(tmp).map {
      value => {
        val ans = value._2._1
        if (value._2._2.isDefined) ans.setTree(value._2._2.get)
        ans
      }
    }.map {
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
          ExtractorWideDeepFea.single(clicked(clk), hash.value, clk, sameCnt, dupAd, dupUser)
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
    val conf = new SparkConf().setAppName("Calc Samples")

    val sc = new SparkContext(conf)
    /*
    val dataPath = Paths.basePath + "/data_2days_all"
    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 28, 18), classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 2), classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])
    */

    // val dataPath = Paths.basePath + "/final_instance_2days_all"
    val dataPath = Paths.basePath + "/final_instance/" + "/2days_all"
    val trainData = sc.thriftParquetFile(dataPath + "/train", classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(dataPath + "/validation", classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val basePath = Paths.basePath + "/base"
    val appCategory = sc.thriftParquetFile(basePath + "/app_category", classOf[AppCategory])
    val helper = DataUtils.calcDatasetHelper(sc, appCategory)

    val treePath = Paths.basePath + "/tree_features"
    // val treeFeatures = loadTreeFeatures(sc, treePath + "/v3", 100)
    val treeFeatures = loadTreeFeatures(sc, treePath + "/v3_deep", 300)

    val testDupAd = findDupAd(testData).collect().toMap
    val testDupUser = findDupUser(testData).collect().toMap

    val trainSamples = calcSamples(sc, treeFeatures, trainData, testDupAd, testDupUser, helper)
    val validationSamples = calcSamples(sc, treeFeatures, validationData, testDupAd, testDupUser, helper)
    val testSamples = calcSamples(sc, treeFeatures, testData, testDupAd, testDupUser, helper)

    val output = Paths.basePath + "/2days" + "/v3_tree_deep_300_stats_wide_deep"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    outputSamples(sc, trainSamples, validationSamples, testSamples, output)

    sc.stop()
    println("Job done!")
  }
}
