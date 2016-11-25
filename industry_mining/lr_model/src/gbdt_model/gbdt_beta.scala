package gbdt_model

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

object gbdt_beta {
  val metric_fileName = "tracelog/v2/join_data/2016-10-2*/,tracelog/v2/join_data/2016-11-0*/"
  val metric_testFileName = "tracelog/v2/join_data/2016-11-13/"
  val debugFileName = "tracelog/v2/join_data/2016-11-03/"
  val model_feature = "gbdtbeta"
  val metric_modelTrees = 30
  val metric_treeDepth = 3

  val sexArray = Array[Int](1, 2)
  val osArray = Array[Int](1, 2)
  val wifiArray = Array[Int](1, 2)
  val slotArray = Array[String]("2-4", "1-3", "2-12", "1-10", "1-30")
  val industryFirstArray = Array[String]("101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
    "111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
    "121", "122", "123", "124", "125", "126", "127", "128", "129", "999")
  val provinceArray = Array[String](
    "110000", "120000", "130000", "140000", "150000", "210000", "220000", "230000", "310000", "320000",
    "330000", "340000", "350000", "360000", "370000", "410000", "420000", "430000", "440000", "450000",
    "460000", "500000", "510000", "520000", "530000", "540000", "610000", "620000", "630000", "640000",
    "650000", "710000", "810000", "820000")
  val jobArray = Array[String]("I1","I2","I3","I4","I5","I6","I7","I8","I9")
  val ideaBirthArray = (0 to 290).toArray
  val ideaDaysArray = (0 to 10).toArray
  val userAdDisplayArray = (0 to 50).toArray
  val userAdClickArray = (0 to 50).toArray

  val sexSize = sexArray.length + 1
  val osSize = osArray.length + 1
  val wifiSize = wifiArray.length + 1
  val slotSize = slotArray.length + 1
  val industryFirstSize = industryFirstArray.length + 1
  val provSize = provinceArray.length + 1
  val hourSize = 24
  val ageSize = 100
  val ideaBirthSize = ideaBirthArray.length + 1
  val ideaDaysSize = ideaDaysArray.length + 1
  val userAdDisplaySize = userAdDisplayArray.length + 1
  val userAdClickSize = userAdClickArray.length + 1
  val jobSize = jobArray.length + 1

  val sexIndustryFirstSize: Int = sexSize * industryFirstSize
  val wifiIndustryFirstSize: Int = wifiSize * industryFirstSize
  val osIndustryFirstSize = osSize * industryFirstSize
  val jobInduSize = jobSize * industryFirstSize
  val osSlotSize: Int = osSize * slotSize
  val wifiSlotSize: Int = wifiSize * slotSize
  val sexSlotSize: Int = sexSize * slotSize
  val ageSlotSize: Int = ageSize * slotSize
  val provSlotSize: Int = provSize * slotSize
  val hourSlotSize: Int = slotSize * hourSize

  val featureSlotSize = List.fill(metric_modelTrees)(1 << metric_treeDepth).toArray[Int] ++
    Array(provSize, industryFirstSize, slotSize, jobSize, sexIndustryFirstSize,
    osSlotSize, wifiSlotSize, hourSlotSize, ideaBirthSize, sexSlotSize, ageSlotSize, provSlotSize, osIndustryFirstSize,
    wifiIndustryFirstSize, jobInduSize)

  val featureSlotCount = featureSlotSize.length
  val featureVal = List.fill(featureSlotCount)(1.0).toArray[Double]



  val featureLen = metric_modelTrees * (1 << metric_treeDepth) + featureSlotSize.sum

  def getIdx(arr: Array[Int], id: Int): Int = {
    var idx = arr.indexOf(id)
    if (idx == -1) {
      idx = arr.length
    }
    idx
  }

  def getIdx(arr: Array[String], id: String): Int = {
    var idx = arr.indexOf(id)
    if (idx == -1) {
      idx = arr.length
    }
    idx
  }

  def combineIdx(arr1_idx: Int, arr2_idx: Int, arr2_len: Int): Int = {
    if (arr1_idx == -1 || arr2_idx == -1) {
      return -1
    }
    arr1_idx * arr2_len + arr2_idx
  }

  def mkLrFeature(model: GradientBoostedTreesModel, ext:(LabeledPoint,Array[Int])): LabeledPoint = {
    var idxArr = getTreeIdxArr(model, ext._1.features)
    idxArr = idxArr ++ ext._2
    val feature = mkFeatureArr(idxArr, featureSlotSize)
    LabeledPoint(ext._1.label, Vectors.sparse(featureLen, feature, featureVal))
  }

  def getTreeIdxArr(model: GradientBoostedTreesModel, curFeature: org.apache.spark.mllib.linalg.Vector): Array[Int] = {
    //val curFeature = Vectors.dense(Array[Double](10))
    val treeCnt = model.numTrees
    var idxArr = Array[Int]()
    for (i <- 0 to treeCnt-1) {
      var idx = 0
      val curDepth = model.trees(i).depth
      var curNode = model.trees(i).topNode
      for (j <- 0 to curDepth-1) {
        val fno = curNode.split.get.feature
        val bar = curNode.split.get.threshold
        if (curFeature(fno) <= bar) {
          curNode = curNode.leftNode.get
        } else {
          idx = idx | (1 << (curDepth - 1 - j))
          curNode = curNode.rightNode.get
        }
      }
      idxArr = idxArr :+ idx
    }
    idxArr
  }

  def calculateCombFeatureWeightIdx(arr1_idx: Int, arr2_idx: Int, arr2_len: Int): Int = {
    if (arr1_idx == -1 || arr2_idx == -1) {
      return -1
    }
    arr1_idx * arr2_len + arr2_idx
  }

  def getDisplayCtrIdx(displaySize:Int, ctrSize:Int, displayUnit:Double,
                       ctrUnit:Double, display:Double, click:Double): (Int,Int) ={
    var ctrIdx = -1
    var displayIdx = -1
    if (display <= 0.0) {
      ctrIdx = ctrSize - 1
      displayIdx = displaySize - 1
    }
    else {
      displayIdx = (display / displayUnit).toInt
      if (displayIdx > displaySize - 2) {
        displayIdx = displaySize - 2
      }
      val ctr = click / display
      ctrIdx = (ctr / ctrUnit).toInt
      if (ctrIdx > ctrSize - 2) {
        ctrIdx = ctrSize - 2
      }
    }
    (displayIdx, ctrIdx)
  }

  def mkFeatureArr(idxArr: Array[Int], sizeArr: Array[Int]): Array[Int] = {
    var tempSize: Int = 0
    val len: Int = idxArr.length
    var featureArr: ArrayBuffer[Int] = ArrayBuffer[Int]()
    var i: Int = 0
    while (i < len) {
      featureArr += (idxArr(i) + tempSize)
      tempSize += sizeArr(i)
      i += 1
    }
    featureArr.toArray
  }

  def extractFeature(r: Array[String]): (LabeledPoint,Array[Int]) = {
    implicit val formats = DefaultFormats

    val label = r(0).toDouble
    val featureBlob = parse(r(2))
    val posDisplay = (featureBlob \ "posDisplay2").extract[Double]
    val posClick = (featureBlob \ "posClick2").extract[Double]
    val userDisplay = (featureBlob \ "userDisplay").extract[Double]
    val userClick = (featureBlob \ "userClick").extract[Double]
    val userAdDisplay = (featureBlob \ "userAdDisplay").extract[Double]
    val userAdClick = (featureBlob \ "userAdClick").extract[Double]
    val userInduDisplay = (featureBlob \ "userIndustryFirstDisplay").extract[Double]
    val userInduClick = (featureBlob \ "userIndustryFirstClick").extract[Double]

    val sex = (featureBlob \ "sex").extract[Int]
    val os = (featureBlob \ "os").extract[Int]
    val wifiType = (featureBlob \ "wifiType").extract[Int]
    val age = (featureBlob \ "age").extract[Int]
    val queryTime_hour = (featureBlob \ "queryTime_hour").extract[Int]

    val slot = (featureBlob \ "slotId").extract[String]
    val industryFirstStr = (featureBlob \ "industryFirst").extract[String]
    val provCode = (featureBlob \ "provCode").extract[String]
    val ideaId = (featureBlob \ "ideaId").extract[String]
    val job = (featureBlob \ "job").extract[String]

    val userCtr = if (userDisplay == 0.0) -1 else userClick/userDisplay
    val posCtr = if (posDisplay == 0.0) -1 else posClick/posDisplay
    val userAdCtr = if (userAdDisplay == 0.0) -1 else userAdClick/userAdDisplay
    val userInduCtr = if (userInduDisplay == 0.0) -1 else userInduClick/userInduDisplay

    val slot1 = slot.split("-")(0).toDouble
    val slot2 = slot.split("-")(1).toDouble

    val curDateStr = r(1)
    var ideaBirth = -1.0
    var ideaDays = -1.0
    if (ideaId.length > 16) {
      val ideaCreateDate: String = ideaId.substring(1, 15)
      val pattern: String = "yyyyMMddhhmmss"
      val dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)

      try {
        val date: Date = dateFormat.parse("20160101000000")
        ideaBirth = ((dateFormat.parse(ideaCreateDate).getTime - date.getTime) / (1000 * 60 * 60 * 24)).toDouble
      } catch {
        case e: Exception => {}
      }
      val pattern2: String = "yyyy-MM-dd hh:mm:ss"
      val dateFormat2: SimpleDateFormat = new SimpleDateFormat(pattern2)
      try {
        val curDate: Date = dateFormat2.parse(curDateStr)
        ideaDays = ((curDate.getTime - dateFormat.parse(ideaCreateDate).getTime) / (1000 * 60 * 60 * 24)).toDouble
      } catch {
        case e: Exception => {}
      }
    }

    val sexVal = sex.toDouble
    val osVal = os.toDouble
    val wifiVal = wifiType.toDouble
    val hourVal = queryTime_hour.toDouble
    val ageVal = age.toDouble

    val gbdtFeature = Array[Double](posCtr, userCtr, userAdCtr, userInduCtr, posDisplay, posClick,
      userDisplay, userClick, userAdDisplay, userAdClick, userInduDisplay, userInduClick, slot1, slot2,
      ideaBirth, ideaDays, sexVal, osVal, wifiVal, hourVal, ageVal)

    val provIdx = getIdx(provinceArray,provCode)
    val industryFirstIdx: Int = getIdx(industryFirstArray,industryFirstStr)
    val slotIdx: Int = getIdx(slotArray,slot)
    val jobIdx = getIdx(jobArray, job)
    val ideaBirthIdx = getIdx(ideaBirthArray, ideaBirth.toInt)
    val ideaDaysIdx = getIdx(ideaDaysArray, ideaDays.toInt)
    val sexIdx: Int = getIdx(sexArray,sex)
    val osIdx: Int = getIdx(osArray,os)
    val wifiIdx: Int = getIdx(wifiArray,wifiType)

    val hourIdx = queryTime_hour

    var ageIdx = age
    if(age > 99 || age < 0) {
      ageIdx = 99
    }

    val jobInduIdx = combineIdx(jobIdx, industryFirstIdx, industryFirstSize)
    val sexIndustryFirstIdx: Int = combineIdx(sexIdx, industryFirstIdx, industryFirstSize)
    val wifiIndustryFirstIdx: Int = combineIdx(wifiIdx, industryFirstIdx, industryFirstSize)
    val osIndustryFirstIdx: Int = combineIdx(osIdx, industryFirstIdx, industryFirstSize)
    val osSlotIdx: Int = combineIdx(osIdx, slotIdx, slotSize)
    val wifiSlotIdx: Int = combineIdx(wifiIdx, slotIdx, slotSize)
    val sexSlotIdx: Int = combineIdx(sexIdx, slotIdx, slotSize)
    val ageSlotIdx: Int = combineIdx(ageIdx, slotIdx, slotSize)
    val provSlotIdx: Int = combineIdx(provIdx, slotIdx, slotSize)
    val hourSlotIdx: Int = combineIdx(hourIdx, slotIdx, slotSize)
/*

*/
    val lrFeature = Array[Int](provIdx, industryFirstIdx, slotIdx, jobIdx, sexIndustryFirstIdx,
      osSlotIdx, wifiSlotIdx, hourSlotIdx, ideaBirthIdx, sexSlotIdx, ageSlotIdx, provSlotIdx, osIndustryFirstIdx,
      wifiIndustryFirstIdx, jobInduIdx)

    (LabeledPoint(label, Vectors.dense(gbdtFeature)),lrFeature)
  }

  def mkDebugBuf(predictionsAndLabels: RDD[(Double, Double)]): Array[String] = {
    predictionsAndLabels.cache()
    val metric_trainAuc = new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC.toString
    val metric_trainMse = predictionsAndLabels.map { x => (x._1 - x._2) * (x._1 - x._2)}.mean().toString
    val metric_trainOe = predictionsAndLabels.map { x => x._1 - x._2}.sum().toString
    predictionsAndLabels.unpersist()

    Array("auc", metric_trainAuc, "mse", metric_trainMse, "oe", metric_trainOe)
  }

  def main(args: Array[String]) {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = new Date()
    val suffix = dateFormat.format(dt)
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName + suffix))
    // train

    val rawTrainingData = sc.textFile(metric_fileName).sample(false, 0.2, System.currentTimeMillis())
    val recordsTrain = rawTrainingData.map(line => line.split("\t")).filter(x => x.length == 3 && x(2).takeRight(1)=="}")
    val extraction = recordsTrain.map(extractFeature)
    extraction.repartition(1000)
    extraction.cache()
    val gbdtTraining = extraction.map(_._1)
    gbdtTraining.cache()

    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(metric_modelTrees)
    //boostingStrategy.setValidationTol(0.00001)
    boostingStrategy.getTreeStrategy.setMaxDepth(metric_treeDepth)
    //boostingStrategy.getTreeStrategy.setMinInstancesPerNode(10)
    val model = GradientBoostedTrees.train(gbdtTraining, boostingStrategy)

    val metric_trainCount = gbdtTraining.count().toString
    val modelName = suffix + model_feature
    val modelPath = "training_pipeline/gbdt_model/" + modelName

    // training auc
    val plGbdt = gbdtTraining.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }

    val debugTrainBuf = mkDebugBuf(plGbdt)
    //gbdtTraining.unpersist()

    // test

    val rawTestData = sc.textFile(metric_testFileName)
    val recordsTest = rawTestData.map(line => line.split("\t"))
    val testExtraction = recordsTest.filter(x => x.length == 3 && x(2).takeRight(1)=="}").map {extractFeature}
    testExtraction.cache()
    val testData = testExtraction.map(_._1)

    val metric_testCount = testData.count().toString

    val testPlGbdt = testData.map { point =>
      val p = model.predict(point.features)
      (p, point.label)
    }

    val debugTestBuf = mkDebugBuf(testPlGbdt)
    //testData.unpersist()

    var debugBuf = Array("train:", metric_fileName, metric_trainCount, "test:", metric_testFileName,
      metric_testCount, "trainingData:")

    debugBuf = debugBuf ++ debugTrainBuf
    debugBuf = debugBuf :+ "testData:"
    debugBuf = debugBuf ++ debugTestBuf

    val lrFeature = extraction.map(x => mkLrFeature(model, x))

    val algorithm = new LogisticRegressionWithLBFGS()
    algorithm.setIntercept(true)

    val lrModel = algorithm.run(lrFeature)
    lrModel.clearThreshold()

    val pl = lrFeature.map { point =>
      val prediction = lrModel.predict(point.features)
      (prediction, point.label)
    }

    val db = mkDebugBuf(pl)

    val testLrFeature = testExtraction.map(x => mkLrFeature(model, x))
    val testPl = testLrFeature.map { point =>
      val prediction = lrModel.predict(point.features)
      (prediction, point.label)
    }
    val testDb = mkDebugBuf(testPl)

    debugBuf = debugBuf :+ "trainData:"
    debugBuf = debugBuf ++ db
    debugBuf = debugBuf :+ "testData:"
    debugBuf = debugBuf ++ testDb

    model.save(sc, modelPath)
    lrModel.save(sc, modelPath + "Lr")
    sc.parallelize(debugBuf,1).saveAsTextFile("training_pipeline/gbdt_model_metadata/"+modelName)
  }
}
