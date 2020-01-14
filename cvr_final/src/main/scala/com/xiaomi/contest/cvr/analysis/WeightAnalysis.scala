package com.xiaomi.contest.cvr.analysis

import com.xiaomi.contest.cvr.utils.Paths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object WeightAnalysis {
  def weightAnalysis(sc: SparkContext, feature_path: String, model_path: String, output_path: String): Unit = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(output_path), true)

    val feature = sc.textFile(feature_path).map(_.split('\t')).map(it => (it(0), it(1)))
    val model = sc.textFile(model_path).map(_.split('\t')).map(it => (it(0), it(1)))

    val ans = model.leftOuterJoin(feature).filter(_._2._2.isDefined).map(it => (it._2._2.get, it._2._1.toDouble)).sortBy(_._2).map(it => it._1 + "\t" + it._2)
    feature.saveAsTextFile(output_path + "/feature")
    model.saveAsTextFile(output_path + "/model")
    ans.saveAsTextFile(output_path + "/ans")
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: feature_path, model_path")
    val Array(feature_path, model_path) = args
    val conf = new SparkConf().setAppName("Weight Analysis")

    val sc = new SparkContext(conf)
    val output_path = Paths.basePath + "/analysis"
    weightAnalysis(sc, feature_path, model_path, output_path + "/lr_weight")

    sc.stop()
    println("Job done!")
  }
}
