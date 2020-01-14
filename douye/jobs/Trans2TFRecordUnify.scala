package com.xiaomi.miui.ad.tf.ofmi.transV2

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import com.xiaomi.miui.ad.tf.ofmi.util.Common._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import org.tensorflow.example.{Example, Feature, Features, Int64List}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-20
  */
object Trans2TFRecordUnify {
  def main(args: Array[String]) {

    val Array(feaPath, idsPath, output_path) = args

    val sparkConf = new SparkConf().setAppName("TFRecord Feature File Demo")
    sparkConf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(sparkConf)

    val par = new HashPartitioner(1000)

    val fea2id = sc.textFile(idsPath)
      .map(e => (e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val feaData = sc.textFile(feaPath)
      .map {
        case e=> {
          val segs = e.split("\t")
          val key = segs(0) + "\t" + segs(1) + "\t" + segs(2) + "\t" + segs(3) + "\t" + segs(4) + "\t" + segs(5)
          (segs(6), key)
        }
      }
      .partitionBy(par)
      .flatMap(e=>for (fea<-e._1.split(" ")) yield (fea.split(":")(0), e._2))
      .leftOuterJoin(fea2id, 1000)
      .map{
        case (fea, (info, feaid)) => {
          if (feaid.isDefined) {
            (info, feaid.get)
          } else {
            (info, "N")
          }
        }
      }
      .reduceByKey(_+CTRL_A+_)
      .map{
        case (info, feaids) => {
          val label = Int64List.newBuilder().addValue(info.split("\t")(0).toLong).build()
          val instanceId = Int64List.newBuilder().addValue(info.split("\t")(2).toLong).build()
          val features = Features.newBuilder()
            .putFeature("iid",Feature.newBuilder().setInt64List(instanceId).build())
            .putFeature("label", Feature.newBuilder().setInt64List(label).build())

          val ids = new ListBuffer[java.lang.Long]()

          for (id <- feaids.split(CTRL_A)) {
            if (!id.equals("N")) {
              ids.append(id.toLong)
            }
          }

          if (ids.size == 0) {
            ids.append(0l)
          }

          val ids_feature = Int64List.newBuilder().addAllValue(ids.asJava)

          features
            .putFeature("fid", Feature.newBuilder().setInt64List(ids_feature).build())


          val example = Example.newBuilder()
            .setFeatures(features.build())
            .build()
          (new BytesWritable(example.toByteArray), NullWritable.get())
        }
      }
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](output_path)

  }
}
