package Data

import java.util

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Util.Common._

import scala.collection.JavaConversions.mapAsJavaMap

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-23
  */
object UserQueryDailyDataCT {
  def isRelated(str: String, str1: String):Boolean = {
    var count = 0
    for (s<-str.split(",")) {
      for (s1<-str.split(",")) {
        if (s.equals(s1)) {
          count += 1
        }
      }
    }
    if (count >= 2) {
      return true
    }
    return false
  }

  def getRelatedApp(qr: String, dis2appids: java.util.HashMap[String, String]) : String = {
    var res = ""
    val iter = dis2appids.entrySet().iterator()
    while(iter.hasNext) {
      val dis2appid = iter.next()
      val dis = dis2appid.getKey
      if (isRelated(qr, dis)) {
        res += dis2appid.getValue + ","
      }
    }
    if (res.length > 2) {
      return res.substring(0, res.length -1)
    } else {
      return ""
    }
  }

  def main (args: Array[String]) {
    val Array(userQueryPath, appInfoPath, outputfiles, ds) = args
    val sparkConf = new SparkConf()
      .setAppName("AdInfoFeatureGeneration").set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "8").set("spark.cores.max", "32")
      .set("spark.akka.frameSize", "100").set("spark.shuffle.manager", "SORT")
      .set("spark.yarn.executor.memoryOverhead", "2047")
      .set("spark.yarn.driver.memoryOverhead", "896")
      .set("spark.memory.fraction", "0.3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "Util.DataClassRegistrator")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }
    println("master: " + sparkConf.get("spark.master"))
    val sc = new SparkContext(sparkConf)

    val par = new HashPartitioner(100)

//    val dis2appid = sc.textFile(appInfoPath)
//      .map(e=>(e.split("\t")(1), e.split("\t")(2)))
//      .collectAsMap()

    val term2appids = sc.textFile(appInfoPath)
      .flatMap(e => for (term <- e.split("\t")(1).split(",")) yield (term + CTRL_A + e.split("\t")(0)))
      .distinct()
      .map(e=>(e.split(CTRL_A)(0) , e.split(CTRL_A)(1)))
      .reduceByKey(_ + "," + _)
      .filter(_._2.split(",").length <= 800)
      .filter(_._2.split(",").length >= 10)
      .collectAsMap()


    val validTerms = sc.textFile(userQueryPath)
      .flatMap(e=> for (term<-e.split("\t")(1).split(",")) yield (term, 1l))
      .reduceByKey(_+_)
      .filter(_._2 <= 200000)
      .filter(_._2 >= 100)
      .collectAsMap()

//    val data = sc.textFile(userQueryPath)
//      .flatMap(e=>for(term<-e.split("\t")(1).split(",")) yield (e.split("\t")(0), term))
//      .partitionBy(par)
//      .filter{
//        case e=> {
//          if (validTerms.contains(e._2))
//            true
//          else
//            false
//        }
//      }
//      .reduceByKey(_+","+_)
//      .map(e=>(e._1, getRelatedApp(e._2, mapAsJavaMap(dis2appid).asInstanceOf[util.HashMap[String, String]])))
//      .map(e=>(ds + "\t" + e._1 + "\t" + e._2))
//      .saveAsTextFile(outputfiles)

    val data = sc.textFile(userQueryPath)
      .flatMap(e=>for(term<-e.split("\t")(1).split(",")) yield (e.split("\t")(0), term))
      .partitionBy(par)
      .map{
        case (imei, term) =>{
          if (validTerms.contains(term) && term2appids.contains(term)) {
            (imei, term2appids.get(term).get)
          } else {
            null
          }
        }
      }
      .filter(_!=null)
      .flatMap(e=>for(appid<-e._2.split(",")) yield (e._1 +"\t"+ appid, 1l))
      .reduceByKey(_+_)
      .filter(_._2 > 3)
      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1)))
      .reduceByKey(_+","+_)
          .map(e=>(ds + "\t" + e._1 + "\t" + e._2))
      .repartition(10)
      .saveAsTextFile(outputfiles)
  }
}
