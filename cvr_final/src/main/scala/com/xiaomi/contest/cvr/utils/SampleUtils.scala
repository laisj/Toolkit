package com.xiaomi.contest.cvr.utils

import org.tensorflow.example._
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import com.xiaomi.contest.cvr.samples.{BaseFea, FeaType, Sample}
import com.xiaomi.contest.cvr.stats.{FeatureStats, SampleStats}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.JavaConversions._

object SampleUtils {
  def calcStats(sc: SparkContext, fea: Array[(String, Int)], output: String): Map[String, Int] = {
    val feaCount = fea.length
    val counters = Array[Long](feaCount)
    sc.parallelize(counters, 1).saveAsTextFile(output + "/counters")
    val feaId = fea.map(_._1).zipWithIndex.map(it => (it._1, it._2.toInt))
    sc.parallelize(feaId.map(it => it._1 + "\t" + it._2), 100).saveAsTextFile(output + "/fea_id")
    feaId.toMap
  }

  def calcStatsWide(sc: SparkContext, fea: Array[(String, Int)], output: String): Map[String, Int] = {
    val totalCount = fea.length
    val counters = Array[String]("%d".format(totalCount))
    sc.parallelize(counters, 1).saveAsTextFile(output + "/counters")
    val feaId = fea.map(_._1).zipWithIndex.map(it => (it._1, it._2.toInt))
    sc.parallelize(feaId.map(it => it._1 + "\t" + it._2), 100).saveAsTextFile(output + "/fea_id")
    feaId.toMap
  }

  def calcFeaturesWide(sc: SparkContext, data: RDD[Sample], output: String): (Map[String, Int], Map[String, Int]) = {
    val fea = data.flatMap(it => it.getFeatures).map(it => (it.getFea, 1)).reduceByKey(_ + _, 800).collect().filter(_._2 >= 100)
    val treeFea = data.flatMap(it => it.getTree_features).map(it => (it.getFea, 1)).reduceByKey(_ + _, 800).collect()

    sc.parallelize(fea.map(it => it._1 + "\t" + it._2)).saveAsTextFile(output + "/fea")
    sc.parallelize(treeFea.map(it => it._1 + "\t" + it._2)).saveAsTextFile(output + "/tree_fea")

    val totalFeaCount = fea.length + treeFea.length
    val counters = Array[String](totalFeaCount + "\t" + fea.length + "\t" + treeFea.length)
    sc.parallelize(counters, 1).saveAsTextFile(output + "/counters")
    val feaId = fea.map(_._1).zipWithIndex.map(it => (it._1, it._2.toInt))
    val treeFeaId = treeFea.map(_._1).zipWithIndex.map(it => (it._1, it._2.toInt))
    sc.parallelize(feaId.map(it => it._1 + "\t" + it._2), 100).saveAsTextFile(output + "/fea_id")
    sc.parallelize(treeFeaId.map(it => it._1 + "\t" + it._2), 100).saveAsTextFile(output + "/tree_fea_id")
    (feaId.toMap, treeFeaId.toMap)
  }

  def calcFeatures(sc: SparkContext, data: RDD[Sample], output: String): Array[(String, Int)] = {
    val features = data.flatMap(_.getFeatures)
    val fea = features.map(it => (it.getFea, 1)).reduceByKey(_ + _, 800)
    val ans = fea.collect()
    fea.map(it => it._1 + "\t" + it._2).saveAsTextFile(output + "/fea")
    val feaHash = features.map(it => (FeatureHash.asUnsignedLongString(it.getIdentifier), it.getFea)).distinct(800)
    feaHash.map(it => it._1 + "\t" + it._2).saveAsTextFile(output + "/fea_hash")
    val feaCount = fea.keys.filter(_.contains("#")).map(it => (it.split("#")(0), 1)).reduceByKey(_ + _, 800)
    feaCount.map(it => it._1 + "\t" + it._2).saveAsTextFile(output + "/fea_count")
    ans
  }

  def calcFeaturesSimple(sc: SparkContext, data: RDD[Sample], output: String): RDD[(String, Int)] = {
    val features = data.flatMap(_.getFeatures)
    val fea = features.map(it => (it.getFea, 1)).reduceByKey(_ + _, 800)
    fea.map(it => it._1 + "\t" + it._2).saveAsTextFile(output + "/fea")
    fea
  }

  def calcFeatureStats(sc: SparkContext, data: RDD[Sample], output: String): SampleStats = {
    val tmp = data.flatMap(_.getFeatures).map(it => (it.getFea, it.getValue)).groupByKey(800).filter(it => it._2.size >= 10).map {
      value => {
        val tmp = value._2.toArray
        val mean = tmp.sum / tmp.length
        val dev = tmp.map(score => (score - mean) * (score - mean))
        val stddev = Math.sqrt(dev.sum / (tmp.length - 1))
        val feaStats = new FeatureStats()
        feaStats.setCnt(tmp.length)
        feaStats.setMin(tmp.min)
        feaStats.setMax(tmp.max)
        feaStats.setMean(mean)
        feaStats.setStddev(stddev)
        (value._1, feaStats)
      }
    }

    val feaStats = tmp.collect()
    sc.parallelize(feaStats.map(it => it._1 + "\t" + it._2.getCnt + "\t" + it._2.toString), 100).saveAsTextFile(output + "/fea_stats")

    val feaId = feaStats.map(_._1).zipWithIndex.map(it => (it._1, it._2.toInt))
    sc.parallelize(feaId.map(it => it._1 + "\t" + it._2), 100).saveAsTextFile(output + "/fea_id")

    val feaIdCount = feaId.length
    val counters = Array[Long](feaIdCount)
    sc.parallelize(counters, 1).saveAsTextFile(output + "/counters")

    val ans = new SampleStats()
    feaId.foreach(it => ans.putToFeatureId(it._1, it._2))
    feaStats.foreach(it => ans.putToFeatureStats(it._1, it._2))
    ans
  }

  def transFeaturesNormal(features: List[BaseFea], stats: SampleStats): List[BaseFea] = {
    features.filter(fea => stats.getFeatureId.contains(fea.getFea)).map {
      fea => {
        val id = stats.getFeatureId.get(fea.getFea).toInt
        fea.setId(id)
        if (fea.getType == FeaType.Continuous) {
          val tmp = stats.getFeatureStats.get(fea.getFea)
          // val value = fea.getValue / (1.0 + math.abs(fea.getValue))
          // val value = (fea.getValue - tmp.getMin) / (tmp.getMax - tmp.getMin)
          val value = (fea.getValue - tmp.getMean) / tmp.getStddev
          fea.setValue(value)
        }
        fea
      }
    }
  }

  def transSamplesNormal(sc: SparkContext, data: RDD[Sample], stats: SampleStats): RDD[Sample] = {
    val hash = sc.broadcast(stats)
    data.map {
      sample => {
        val features = transFeaturesNormal(sample.getFeatures.toList, hash.value)
        sample.setFeatures(features)
        sample
      }
    }
  }

  def transFeatures(features: List[BaseFea], stats: Map[String, Int]): List[BaseFea] = {
    features.filter(fea => stats.contains(fea.getFea)).map {
      fea => {
        val id = stats(fea.getFea)
        fea.setId(id)
        if (fea.getType == FeaType.Continuous) {
          val value = fea.getValue / (1.0 + math.abs(fea.getValue))
          fea.setValue(value)
        }
        fea
      }
    }
  }

  def transSamplesWide(sc: SparkContext, data: RDD[Sample], stats: (Map[String, Int], Map[String, Int])): RDD[Sample] = {
    val hash = sc.broadcast(stats)
    data.map {
      sample => {
        val features = transFeatures(sample.getFeatures.toList, hash.value._1)
        sample.setFeatures(features)
        val treeFeatures = transFeatures(sample.getTree_features.toList, hash.value._2)
        sample.setFeatures(treeFeatures)
        sample
      }
    }
  }

  def transSamples(sc: SparkContext, data: RDD[Sample], stats: Map[String, Int]): RDD[Sample] = {
    val hash = sc.broadcast(stats)
    data.map {
      sample => {
        val features = transFeatures(sample.getFeatures.toList, hash.value)
        sample.setFeatures(features)
        sample
      }
    }
  }

  def encodeFeatures(features: List[BaseFea], stats: Map[String, Int]): List[BaseFea] = {
    features.filter(fea => stats.contains(fea.getFea)).map {
      fea => {
        val id = stats(fea.getFea)
        fea.setId(id)
        fea
      }
    }
  }

  def encodeSamples(sc: SparkContext, data: RDD[Sample], stats: Map[String, Int]): RDD[Sample] = {
    val hash = sc.broadcast(stats)
    data.map {
      sample => {
        val features = encodeFeatures(sample.getFeatures.toList, hash.value)
        sample.setFeatures(features)
        sample
      }
    }
  }

  def outputDnnSamples(sc: SparkContext, samples: RDD[Sample], output: String): Unit = {
    val data = samples.map {
      sample => {
        val label = FloatList.newBuilder().addValue(sample.getLabel.toFloat).build()
        val instanceId = Int64List.newBuilder().addValue(sample.getInstance_id).build()
        val featureId = Int64List.newBuilder()
        val value = FloatList.newBuilder()
        sample.getFeatures.sortBy(_.getId).foreach {
          mm => {
            featureId.addValue(mm.getId)
            value.addValue(mm.getValue.toFloat)
          }
        }
        val features = Features.newBuilder()
            .putFeature("label", Feature.newBuilder().setFloatList(label).build())
            .putFeature("instance_id", Feature.newBuilder().setInt64List(instanceId).build())
            .putFeature("feature_id", Feature.newBuilder().setInt64List(featureId.build()).build())
            .putFeature("value", Feature.newBuilder().setFloatList(value.build()).build())
            .build()
        val example = Example.newBuilder()
            .setFeatures(features)
            .build()
        (new BytesWritable(example.toByteArray), NullWritable.get())
      }
    }
    data.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](output)
  }

  def outputDnnWideSamples(sc: SparkContext, samples: RDD[Sample], output: String): Unit = {
    val data = samples.map {
      sample => {
        val label = FloatList.newBuilder().addValue(sample.getLabel.toFloat).build()
        val instanceId = Int64List.newBuilder().addValue(sample.getInstance_id).build()
        val featureId = Int64List.newBuilder()
        val value = FloatList.newBuilder()
        sample.getFeatures.filter(!_.getGroup_name.contains("leaf_indices")).sortBy(_.getId).foreach {
          mm => {
            featureId.addValue(mm.getId)
            value.addValue(mm.getValue.toFloat)
          }
        }
        val treeFeatureId = Int64List.newBuilder()
        val treeValue = FloatList.newBuilder()
        sample.getFeatures.filter(_.getGroup_name.contains("leaf_indices")).sortBy(_.getId).foreach {
          mm => {
            treeFeatureId.addValue(mm.getId)
            treeValue.addValue(mm.getValue.toFloat)
          }
        }
        val features = Features.newBuilder()
          .putFeature("label", Feature.newBuilder().setFloatList(label).build())
          .putFeature("instance_id", Feature.newBuilder().setInt64List(instanceId).build())
          .putFeature("feature_id", Feature.newBuilder().setInt64List(featureId.build()).build())
          .putFeature("value", Feature.newBuilder().setFloatList(value.build()).build())
          .putFeature("tree_feature_id", Feature.newBuilder().setInt64List(treeFeatureId.build()).build())
          .putFeature("tree_value", Feature.newBuilder().setFloatList(treeValue.build()).build())
          .build()
        val example = Example.newBuilder()
          .setFeatures(features)
          .build()
        (new BytesWritable(example.toByteArray), NullWritable.get())
      }
    }
    data.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](output)
  }

  def outputDnnWideSplitSamples(sc: SparkContext, samples: RDD[Sample], output: String): Unit = {
    val data = samples.map {
      sample => {
        val label = FloatList.newBuilder().addValue(sample.getLabel.toFloat).build()
        val instanceId = Int64List.newBuilder().addValue(sample.getInstance_id).build()
        val featureId = Int64List.newBuilder()
        val value = FloatList.newBuilder()
        sample.getFeatures.sortBy(_.getId).foreach {
          mm => {
            featureId.addValue(mm.getId)
            value.addValue(mm.getValue.toFloat)
          }
        }
        val treeFeatureId = Int64List.newBuilder()
        val treeValue = FloatList.newBuilder()
        sample.getTree_features.sortBy(_.getId).foreach {
          mm => {
            treeFeatureId.addValue(mm.getId)
            treeValue.addValue(mm.getValue.toFloat)
          }
        }
        val features = Features.newBuilder()
          .putFeature("label", Feature.newBuilder().setFloatList(label).build())
          .putFeature("instance_id", Feature.newBuilder().setInt64List(instanceId).build())
          .putFeature("feature_id", Feature.newBuilder().setInt64List(featureId.build()).build())
          .putFeature("value", Feature.newBuilder().setFloatList(value.build()).build())
          .putFeature("tree_feature_id", Feature.newBuilder().setInt64List(treeFeatureId.build()).build())
          .putFeature("tree_value", Feature.newBuilder().setFloatList(treeValue.build()).build())
          .build()
        val example = Example.newBuilder()
          .setFeatures(features)
          .build()
        (new BytesWritable(example.toByteArray), NullWritable.get())
      }
    }
    data.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](output)
  }

  def outputLRSamples(sc: SparkContext, samples: RDD[Sample], output: String, with_instance_id: Boolean): Unit = {
    val data = samples.map {
      sample => {
        val features = sample.getFeatures.sortBy(_.getIdentifier).map(fea => fea.getIdentifier).mkString(" ")
        if (with_instance_id) {
          sample.getLabel + " " + features + "#" + sample.getInstance_id
        }
        else {
          sample.getLabel + " " + features
        }
      }
    }
    data.saveAsTextFile(output, classOf[GzipCodec])
  }

  def outputFMSamples(sc: SparkContext, samples: RDD[Sample], output: String, with_instance_id: Boolean): Unit = {
    val data = samples.map {
      sample => {
        val features = sample.getFeatures.map {
          fea => "%s:%d".format(FeatureHash.asUnsignedLongString(fea.getIdentifier), fea.getValue.toInt)
        }.mkString(" ")
        if (with_instance_id) {
          sample.getLabel + " " + features + "#" + sample.getInstance_id
        }
        else {
          sample.getLabel + " " + features
        }
      }
    }
    data.saveAsTextFile(output, classOf[GzipCodec])
  }

  def outputFFMSamples(sc: SparkContext, samples: RDD[Sample], output: String, with_instance_id: Boolean): Unit = {
    val data = samples.map {
      sample => {
        val features = sample.getFeatures.map {
          fea => "%d:%s:%d".format(fea.getGroup_id, FeatureHash.asUnsignedLongString(fea.getIdentifier), fea.getValue.toInt)
        }.mkString(" ")
        if (with_instance_id) {
          sample.getLabel + " " + features + "#" + sample.getInstance_id
        }
        else {
          sample.getLabel + " " + features
        }
      }
    }
    data.saveAsTextFile(output, classOf[GzipCodec])
  }

  def outputFFMSamplesId(sc: SparkContext, samples: RDD[Sample], output: String, with_instance_id: Boolean): Unit = {
    val data = samples.map {
      sample => {
        val features = sample.getFeatures.map {
          fea => "%d:%d:%d".format(fea.getGroup_id, fea.getId, fea.getValue.toInt)
        }.mkString(" ")
        if (with_instance_id) {
          sample.getLabel + " " + features + "#" + sample.getInstance_id
        }
        else {
          sample.getLabel + " " + features
        }
      }
    }
    data.saveAsTextFile(output)
  }

  def outputLibSVMSamples(sc: SparkContext, samples: RDD[Sample], output: String, with_instance_id: Boolean): Unit = {
    val data = samples.map {
      sample => {
        val features = sample.getFeatures.sortBy(_.getId).map {
          fea => "%d:%s".format(fea.getId, fea.getValue.toFloat)
        }.mkString(" ")
        if (with_instance_id) {
          sample.getLabel + " " + features + "#" + sample.getInstance_id
        }
        else {
          sample.getLabel + " " + features
        }
      }
    }
    data.saveAsTextFile(output)
  }

  def libSVM2TFRecords(sc: SparkContext, libSVMPath: String, tfRecordPath: String): Unit = {
    val data = sc.textFile(libSVMPath).map {
      line => {
        val item = line.split('\t')
        val featureList = item(1).split(" ").map(_.split(":")).map(it => (it(0).toInt, it(1).toFloat)).sortBy(_._1)
        val label = FloatList.newBuilder().addValue(item(0).toFloat).build()
        val instanceId = Int64List.newBuilder().addValue(item(2).toInt).build()
        val featureId = Int64List.newBuilder()
        val value = FloatList.newBuilder()
        featureList.foreach {
          mm => {
            featureId.addValue(mm._1)
            value.addValue(mm._2)
          }
        }
        val features = Features.newBuilder()
          .putFeature("label", Feature.newBuilder().setFloatList(label).build())
          .putFeature("instance_id", Feature.newBuilder().setInt64List(instanceId).build())
          .putFeature("feature_id", Feature.newBuilder().setInt64List(featureId.build()).build())
          .putFeature("value", Feature.newBuilder().setFloatList(value.build()).build())
          .build()
        val example = Example.newBuilder()
          .setFeatures(features)
          .build()
        (new BytesWritable(example.toByteArray), NullWritable.get())
      }
    }
    data.saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](tfRecordPath)
  }
}
