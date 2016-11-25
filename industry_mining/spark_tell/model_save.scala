import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

// spark-shell --jars /home/adengine/sijia/scalaredis_all_in_one.jar
import org.apache.spark.mllib.classification.LogisticRegressionModel
val modelNameIn = "20161109104403lr31"
val modelNameOut = "20161109104403lr31"
val sameModel = LogisticRegressionModel.load(sc, "training_pipeline/model/"+modelNameIn)

import com.redis._

val r = new RedisClient("redis.com", 6222)
val weight_length = sameModel.weights.size
r.hmset("model:"+modelNameOut+":info", Map("weight_length" -> weight_length, "intercept" -> sameModel.intercept))

var i = 0
val bucketSize = Math.ceil(weight_length / 1000.0)
while (i < bucketSize) {
  r.hmset("model:"+modelNameOut+":info", Map("weight_" + i -> sameModel.weights.toArray.slice(i*1000, i*1000+1000).mkString(",")))
  i = i+1
}

r.sadd("lrModels", modelNameOut)
r.publish("reloadLrModel", "1")
