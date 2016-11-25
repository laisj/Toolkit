import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

val data = sc.textFile("log/2016-11-20/*/*/display-*,log/2016-11-21/*/*/display-*,log/2016-11-22/*/*/display-*")
val data1 = data.map(_.split("\\|")).filter(_(5)=="2016093017121155930")
val data2 = data1.filter(_(20)!="-").map(x=>((x(0).substring(0,13), x(10)),(x(20).toDouble,x(21).split("cpc:")(1).split(",")(0).toDouble,1)))
val data3 = data2.reduceByKey((x,y)=>(x._1 + y._1,x._2 + y._2,x._3+y._3)).map(x=>(x._1,(x._2._1/x._2._3,x._2._2/x._2._3,x._2._3)))
data3.filter(_._1._2=="2-12").sortBy(_._1._2).sortBy(_._1._1).collect.foreach(println)
