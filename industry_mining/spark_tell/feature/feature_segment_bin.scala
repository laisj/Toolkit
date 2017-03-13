import org.apache.spark.SparkContext._
import sqlContext.implicits._
import sqlContext.sql
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.Searching._
val j = sc.textFile("tracelog/v2/join_data/2016-11-13/")
val j = sc.textFile("tracelog/v2/join_data/2016-11-13/").sample(false, 0.01, System.currentTimeMillis().toInt)
val f = j.map(x=>x.split("\t")(2))
val p =f.map{x=>
implicit val formats = DefaultFormats
((parse(x) \ "posDisplay2").extract[Double],(parse(x) \ "posClick2").extract[Double])
}
val pp = p.filter(_._1!=0.0)
val q = pp.map(x=>x._2/x._1)
val qq = q.sortBy(x=>x)
val qq2 = qq.zipWithIndex.map(x=>(x._2,x._1))
val cnt = qq2.count
val res = (1 to 100).map(x=>qq2.lookup(cnt/100*x))
val res2 = (1 to 10).map(x=>qq2.lookup(cnt/10*x))
res.foreach(x=>println(x(0)))
Array[Double](0,1,2,3).search(2.0).insertionPoint
