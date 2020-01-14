package com.xiaomi.contest.cvr.process

import com.xiaomi.contest.cvr._
import com.xiaomi.contest.cvr.utils.Paths
import com.xiaomi.data.commons.spark.HdfsIO._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object DataFinalInstance {
  def calcFinalInstance(sc: SparkContext, stats: RDD[DatasetStats], data: RDD[CVRInstance]): RDD[CVRInstance] = {
    val statsFea = stats.map(it => (it.getInstance_id, it))
    data.map(it => (it.getData.getInstance_id, it)).leftOuterJoin(statsFea).map {
      value => {
        val ans = value._2._1
        if (value._2._2.isDefined) ans.setData_stats(value._2._2.get)
        ans
      }
    }
  }

  def calc(sc: SparkContext): Unit = {
    val dataPath = Paths.basePath + "/data_2days_all"
    /*
    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 28, 18), classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 2), classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])
    */
    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 29, 19), classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 1), classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val statsPath = Paths.basePath + "/stats_all"
    val statsFeatures = sc.thriftParquetFile(statsPath, classOf[DatasetStats])

    val trainInstance = calcFinalInstance(sc, statsFeatures, trainData)
    val validationInstance = calcFinalInstance(sc, statsFeatures, validationData)
    val testInstance = calcFinalInstance(sc, statsFeatures, testData)

    val output = Paths.basePath + "/final_instance/" + "/1days_all"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)

    trainInstance.saveAsParquetFile(output + "/train")
    validationInstance.saveAsParquetFile(output + "/validation")
    testInstance.saveAsParquetFile(output + "/test")
  }

  def calcLatest(sc: SparkContext): Unit = {
    val dataPath = Paths.basePath + "/data_2days_all"
    val trainData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 20), classOf[CVRInstance])
    val validationData = sc.thriftParquetFile(Paths.multipleDaysPath(dataPath + "/train", 30, 1), classOf[CVRInstance])
    val testData = sc.thriftParquetFile(dataPath + "/test", classOf[CVRInstance])

    val statsPath = Paths.basePath + "/stats_all"
    val statsFeatures = sc.thriftParquetFile(statsPath, classOf[DatasetStats])

    val trainInstance = calcFinalInstance(sc, statsFeatures, trainData)
    val validationInstance = calcFinalInstance(sc, statsFeatures, validationData)
    val testInstance = calcFinalInstance(sc, statsFeatures, testData)

    val output = Paths.basePath + "/final_instance/" + "/0days_all"
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output), true)

    trainInstance.saveAsParquetFile(output + "/train")
    validationInstance.saveAsParquetFile(output + "/validation")
    testInstance.saveAsParquetFile(output + "/test")
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: start_day, days")
    val Array(start_day, days) = args
    val conf = new SparkConf().setAppName("Calc Final Instance")

    val sc = new SparkContext(conf)
    // calc(sc)
    calcLatest(sc)

    sc.stop()
    println("Job done!")
  }
}
