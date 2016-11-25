import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

val displayData = sc.textFile("rtlog/2016-08-23/05/*/adengine/display/")
val exp6dd = displayData.filter{x=>x.split("\\|")(17)=="exp6" && x.split("\\|")(10)!="4-3"}
//exp6dd.map(x=>(x.split("\\|")(5),1)).reduceByKey(_+_).sortBy(-_._2).take(10).foreach(println)

val clickData = sc.textFile("rtlog/2016-08-23/05/*/adengine/click/")
val exp6dc = clickData.filter{x=>x.split("\\|")(17)=="exp6" && x.split("\\|")(10)!="4-3"}
//exp6dc.map(x=>(x.split("\\|")(5),1)).reduceByKey(_+_).sortBy(-_._2).take(10).foreach(println)
exp6dc.map(x=>(x.split("\\|")(9),1)).reduceByKey(_+_).sortBy(-_._2).take(10).foreach(println)

exp6dd.filter(x=>x.split("\\|")(5) == "2016072610111060578").count
exp6dc.filter(x=>x.split("\\|")(5) == "2016072610111060578").count