import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

sc.textFile("log/2016-07-18/*/12/queryResult-*").first

val data = sc.textFile("log/2016-07-18/*/12/queryResult-*").map(x=>x.split("\\|"))
val data2 = data.map(x=>(x(0),x(8),x(9),x(18)))
