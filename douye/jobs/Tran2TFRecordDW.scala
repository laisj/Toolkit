package com.xiaomi.miui.ad.tf.ofmi.transV2

import com.xiaomi.miui.ad.tf.ofmi.util.Common._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.tensorflow.example.{Example, Feature, Features, Int64List}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat

import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-21
  */
object Tran2TFRecordDW {
  def main(args: Array[String]) {

    val Array(feaPath, deepIdPath, wideIdPath, output_path) = args

    val sparkConf = new SparkConf().setAppName("TFRecord Feature File Demo")
    sparkConf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(sparkConf)

    val par = new HashPartitioner(1000)

    val dfea2id = sc.textFile(deepIdPath)
      .map(e => (e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wfea2id = sc.textFile(wideIdPath)
      .map(e => (e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val infos =  sc.textFile(feaPath)
      .map {
        case e => {
          val segs = e.split("\t")
          val key = segs(0) + "\t" + segs(1) + "\t" + segs(2) + "\t" + segs(3) + "\t" + segs(4) + "\t" + segs(5)
          (key, 1l)
        }
      }
      .persist(StorageLevel.MEMORY_AND_DISK)


    val dfeaData = sc.textFile(feaPath)
      .map {
        case e => {
          val segs = e.split("\t")
          val key = segs(0) + "\t" + segs(1) + "\t" + segs(2) + "\t" + segs(3) + "\t" + segs(4) + "\t" + segs(5)
          (segs(6), key)
        }
      }
      .partitionBy(par)
      .flatMap(e => for (fea <- e._1.split(" ")) yield (fea.split(":")(0), e._2))
      .leftOuterJoin(dfea2id, 1000)
      .map {
        case (fea, (info, feaid)) => {
          if (feaid.isDefined) {
            (info, feaid.get)
          } else {
            null
          }
        }
      }
      .filter(_ != null)
      .reduceByKey(_ + CTRL_A + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wfeaData = sc.textFile(feaPath)
      .map {
        case e => {
          val segs = e.split("\t")
          val key = segs(0) + "\t" + segs(1) + "\t" + segs(2) + "\t" + segs(3) + "\t" + segs(4) + "\t" + segs(5)
          (segs(6), key)
        }
      }
      .partitionBy(par)
      .flatMap(e => for (fea <- e._1.split(" ")) yield (fea.split(":")(0), e._2))
      .leftOuterJoin(wfea2id, 1000)
      .map {
        case (fea, (info, feaid)) => {
          if (feaid.isDefined) {
            (info, feaid.get)
          } else {
            null
          }
        }
      }
      .filter(_ != null)
      .reduceByKey(_ + CTRL_A + _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val tfrecord = infos
      .leftOuterJoin(wfeaData, 1000)
      .map{
        case (info, (count, wfea)) => {
          val wIds = new ListBuffer[java.lang.Long]()
          if (wfea.isDefined) {
            for (wId <- wfea.get.split(CTRL_A)) {
              wIds.append(wId.toLong)
            }
          } else {
            wIds.append(0l)
          }
          (info, wIds)
        }
      }
      .leftOuterJoin(dfeaData, 1000)
      .map {
        case (info, (wIds, dfea)) => {
          val label = Int64List.newBuilder().addValue(info.split("\t")(0).toLong).build()
          val instanceId = Int64List.newBuilder().addValue(info.split("\t")(2).toLong).build()
          val features = Features.newBuilder()
            .putFeature("iid", Feature.newBuilder().setInt64List(instanceId).build())
            .putFeature("label", Feature.newBuilder().setInt64List(label).build())

          val dIds = new ListBuffer[java.lang.Long]()

          if (dfea.isDefined) {
            for (dId <- dfea.get.split(CTRL_A)) {
              dIds.append(dId.toLong)
            }
          } else {
            dIds.append(0l)
          }

          val deepfeature = Int64List.newBuilder().addAllValue(dIds.asJava)
          val widefeature = Int64List.newBuilder().addAllValue(wIds.asJava)

          features
            .putFeature("deep", Feature.newBuilder().setInt64List(deepfeature).build())
            .putFeature("wide", Feature.newBuilder().setInt64List(widefeature).build())

          val example = Example.newBuilder()
            .setFeatures(features.build())
            .build()
          (new BytesWritable(example.toByteArray), NullWritable.get())
        }
      }
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](output_path)

  }
}