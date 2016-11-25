/**
  * Created by laisijia on 16/7/28.
  */
import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

val data = sc.textFile("log/2016-07-16/*/22/queryParam-*").repartition(1000).cache()
val data2 = sc.textFile("log/2016-07-16/*/22/queryResult-*").repartition(1000).cache()

val data5 = data.map(x=>x.split("\\|")).filter(x=>x(16)=="2-4,2-12" && x(18)=="exp0").map(x=>(x(3),x))
val data6 = data2.map(x=>x.split("\\|")).filter(x=>x(5)=="2-4" && x(15)=="exp0").map(x=>(x(3),x))

val data7 = data5.subtractByKey(data6)
data7.count


data7.map(x=>(x._2(4),1)).reduceByKey(_+_).sortBy(-_._2).take(200).foreach(println)

data7.map(x=>(x._2(5),1)).reduceByKey(_+_).sortBy(-_._2).take(200).foreach(println)
data7.map(x=>(x._2(13),1)).reduceByKey(_+_).sortBy(-_._2).take(200).foreach(println)
data7.map(x=>(x._2(14),1)).reduceByKey(_+_).sortBy(-_._2).take(200).foreach(println)