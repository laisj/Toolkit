package com.xiaomi.contest.cvr.samples

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.features._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.contest.cvr.utils.{DataUtils, Paths, SampleUtils}

import scala.collection.JavaConversions._

object LrSamples {
  def outputSamples(sc: SparkContext, train: RDD[Sample], validation: RDD[Sample], test: RDD[Sample], output: String): Unit = {
    val statsPath = output + "/stats"
    val trainFeatures = SampleUtils.calcFeatures(sc, train, statsPath + "/train")
    // val validationFeatures = SampleUtils.calcFeatures(sc, validation, statsPath + "/validation")
    val testFeatures = SampleUtils.calcFeatures(sc, test, statsPath + "/test")

    // val trainHash = trainFeatures.toMap
    val trainHash = trainFeatures.filter(it => it._2 >= 5).toMap
    // val validationHash = validationFeatures.toMap

    val features = testFeatures.filter(it => trainHash.contains(it._1))
    /*
    val features = trainFeatures.filter(it => it._2 >= 3)
    */

    val stats = SampleUtils.calcStats(sc, features, statsPath)

    val trainSamples = SampleUtils.transSamples(sc, train, stats)
    val validationSamples = SampleUtils.transSamples(sc, validation, stats)
    val testSamples = SampleUtils.transSamples(sc, test, stats)

    val dnnSamplesPath = output + "/dnn_samples"
    SampleUtils.outputDnnSamples(sc, trainSamples, dnnSamplesPath + "/train")
    SampleUtils.outputDnnSamples(sc, validationSamples, dnnSamplesPath + "/validation")
    SampleUtils.outputDnnSamples(sc, testSamples, dnnSamplesPath + "/test")

    /*
    val lrSamplesPath = output + "/lr_samples"
    SampleUtils.outputLRSamples(sc, trainSamples, lrSamplesPath + "/train", with_instance_id = false)
    SampleUtils.outputLRSamples(sc, validationSamples, lrSamplesPath + "/validation", with_instance_id = false)
    SampleUtils.outputLRSamples(sc, testSamples, lrSamplesPath + "/test", with_instance_id = true)

    val ffmSamplesPath = output + "/ffm_samples"
    SampleUtils.outputFFMSamples(sc, trainSamples, ffmSamplesPath + "/train", with_instance_id = false)
    SampleUtils.outputFFMSamples(sc, validationSamples, ffmSamplesPath + "/validation", with_instance_id = false)
    SampleUtils.outputFFMSamples(sc, testSamples, ffmSamplesPath + "/test", with_instance_id = true)
    */

    val libSVMSamplesPath = output + "/lib_svm_samples"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesPath + "/train", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesPath + "/validation", with_instance_id = false)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesPath + "/test", with_instance_id = false)

    val libSVMSamplesWithIdPath = output + "/lib_svm_samples_with_id"
    SampleUtils.outputLibSVMSamples(sc, trainSamples, libSVMSamplesWithIdPath + "/train", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, validationSamples, libSVMSamplesWithIdPath + "/validation", with_instance_id = true)
    SampleUtils.outputLibSVMSamples(sc, testSamples, libSVMSamplesWithIdPath + "/test", with_instance_id = true)
  }

  def calcSamplesBak(sc: SparkContext, data: RDD[CVRInstance], helper: DatasetHelper): RDD[Sample] = {
    val hash = sc.broadcast(helper)
    data.map {
      value => {
        val ans = ExtractorLrFea.single(value, hash.value)
        ans
      }
    }
  }

  def calcSamples(sc: SparkContext, data: RDD[CVRInstance], helper: DatasetHelper): RDD[Sample] = {
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
          ExtractorDnnFea.single(clicked(clk), hash.value, clk, sameCnt, dupAd = false, dupUser = false)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Calc Samples")

    val sc = new SparkContext(conf)

    val dataPath = Paths.basePath + "/data_2days_all"
    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 28, 18), classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 2), classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val basePath = Paths.basePath + "/base"
    val appCategory = sc.thriftParquetFile(basePath + "/app_category", classOf[AppCategory])
    val helper = DataUtils.calcDatasetHelper(sc, appCategory)

    val trainSamples = calcSamples(sc, trainData, helper)
    val validationSamples = calcSamples(sc, validationData, helper)
    val testSamples = calcSamples(sc, testData, helper)

    val output = Paths.basePath + "/v3_tree"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)
    outputSamples(sc, trainSamples, validationSamples, testSamples, output)

    sc.stop()
    println("Job done!")
  }
}
