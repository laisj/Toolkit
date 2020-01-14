package com.xiaomi.contest.cvr.samples

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features._
import com.xiaomi.contest.cvr.utils.{DataUtils, Paths, SampleUtils}
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FmSamples {
  def outputSamples(sc: SparkContext, train: RDD[Sample], validation: RDD[Sample], test: RDD[Sample], output: String): Unit = {
    val statsPath = output + "/stats"
    val trainFeatures = SampleUtils.calcFeaturesSimple(sc, train, statsPath + "/train")
    val testFeatures = SampleUtils.calcFeaturesSimple(sc, test, statsPath + "/test")

    val trainHash = trainFeatures.filter(it => it._2 >= 100).collect().toMap
    val features = testFeatures.filter(it => trainHash.contains(it._1)).collect()

    val stats = SampleUtils.calcStats(sc, features, statsPath)

    val trainSamples = SampleUtils.encodeSamples(sc, train, stats)
    val validationSamples = SampleUtils.encodeSamples(sc, validation, stats)
    val testSamples = SampleUtils.encodeSamples(sc, test, stats)

    trainSamples.cache()
    validationSamples.cache()
    testSamples.cache()

    val dnnSamplesPath = output + "/dnn_samples"
    SampleUtils.outputDnnSamples(sc, trainSamples, dnnSamplesPath + "/train")
    SampleUtils.outputDnnSamples(sc, validationSamples, dnnSamplesPath + "/validation")
    SampleUtils.outputDnnSamples(sc, testSamples, dnnSamplesPath + "/test")

    /*
    val fmSamplesPath = output + "/fm_samples"
    SampleUtils.outputFMSamples(sc, trainSamples, fmSamplesPath + "/train", with_instance_id = true)
    SampleUtils.outputFMSamples(sc, validationSamples, fmSamplesPath + "/validation", with_instance_id = true)
    SampleUtils.outputFMSamples(sc, testSamples, fmSamplesPath + "/test", with_instance_id = true)
    */

    val ffmSamplesPath = output + "/ffm_samples"
    SampleUtils.outputFFMSamples(sc, trainSamples, ffmSamplesPath + "/train", with_instance_id = true)
    SampleUtils.outputFFMSamples(sc, validationSamples, ffmSamplesPath + "/validation", with_instance_id = true)
    SampleUtils.outputFFMSamples(sc, testSamples, ffmSamplesPath + "/test", with_instance_id = true)

    /*
    val ffmSamplesIdPath = output + "/ffm_samples_id"
    SampleUtils.outputFFMSamplesId(sc, trainSamples, ffmSamplesIdPath + "/train", with_instance_id = false)
    SampleUtils.outputFFMSamplesId(sc, validationSamples, ffmSamplesIdPath + "/validation", with_instance_id = false)
    SampleUtils.outputFFMSamplesId(sc, testSamples, ffmSamplesIdPath + "/test", with_instance_id = false)

    val ffmSamplesIdWithInstanceIdPath = output + "/ffm_samples_id_with_instance_id"
    SampleUtils.outputFFMSamplesId(sc, trainSamples, ffmSamplesIdWithInstanceIdPath + "/train", with_instance_id = true)
    SampleUtils.outputFFMSamplesId(sc, validationSamples, ffmSamplesIdWithInstanceIdPath + "/validation", with_instance_id = true)
    SampleUtils.outputFFMSamplesId(sc, testSamples, ffmSamplesIdWithInstanceIdPath + "/test", with_instance_id = true)
    */

    /*
    val libSVMSamplesPath = output + "/lib_svm_samples"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesPath + "/train", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesPath + "/validation", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesPath + "/test", with_instance_id = false)
    */

    val libSVMSamplesWithInstanceIdPath = output + "/lib_svm_samples_with_instance_id"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesWithInstanceIdPath + "/train", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesWithInstanceIdPath + "/validation", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesWithInstanceIdPath + "/test", with_instance_id = true)
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
          ExtractorFmFea.single(cur, hash.value, clk, sameCnt, dupAd, dupUser)
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

    val dataPath = Paths.basePath + "/final_instance/" + "/0days_all"
    val trainData = sc.thriftParquetFile(dataPath + "/train", classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(dataPath + "/validation", classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val testDupAd = findDupAd(testData).collect().toMap
    val testDupUser = findDupUser(testData).collect().toMap

    val basePath = Paths.basePath + "/base"
    val appCategory = sc.thriftParquetFile(basePath + "/app_category", classOf[AppCategory])
    val helper = DataUtils.calcDatasetHelper(sc, appCategory)

    val treePath = Paths.basePath + "/tree_features"
    val treeFeatures = loadTreeFeatures(sc, treePath + "/v3", 100)
    // val treeFeatures = loadTreeFeatures(sc, treePath + "/v3_deep", 400)

    val trainSamples = calcSamples(sc, treeFeatures, trainData, testDupAd, testDupUser, helper)
    val validationSamples = calcSamples(sc, treeFeatures, validationData, testDupAd, testDupUser, helper)
    val testSamples = calcSamples(sc, treeFeatures, testData, testDupAd, testDupUser, helper)

    val output = Paths.basePath + "/0days" + "/0days_v3_tree_100_fm_stats_100_1"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    outputSamples(sc, trainSamples, validationSamples, testSamples, output)

    sc.stop()
    println("Job done!")
  }
}
