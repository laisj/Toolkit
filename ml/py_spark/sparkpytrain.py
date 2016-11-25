from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils
import json
import datetime

timeStr = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
sc = SparkContext(conf=SparkConf(), appName="lr_2503_"+timeStr)

posDisplaySize = 201
posCtrSize = 26
posDisplayUnit = 1000.0
posCtrUnit = 0.001

userDisplaySize = 21
userCtrSize = 51
userDisplayUnit = 50.0
userCtrUnit = 0.001


def calculateCombFeatureWeightIdx(arr1_idx, arr2_idx, arr2_len):
    if arr1_idx == -1 or arr2_idx == -1:
        return -1
    return arr1_idx * arr2_len + arr2_idx


def getDisplayCtrIdx(displaySize, ctrSize, displayUnit, ctrUnit, display, click):
    if display <= 0.0:
        ctrIdx = ctrSize - 1
        displayIdx = displaySize - 1
    else:
        displayIdx = int(display / displayUnit)
        if displayIdx > displaySize - 2:
            displayIdx = displaySize - 2
        ctr = click / display
        ctrIdx = int(ctr / ctrUnit)
        if ctrIdx > ctrSize - 2:
            ctrIdx = ctrSize - 2
    return displayIdx, ctrIdx


def mkFeatureVector(idxSizeArr):
    tempSize = 0
    featureArr = []
    valueArr = []
    for i in idxSizeArr:
        featureArr.append(i[0] + tempSize)
        valueArr.append(1)
        tempSize += i[1]
    return Vectors.sparse(tempSize, featureArr, valueArr)


def featureExtraction(parseArr):
    sexArr = [1,2]
    slotArr = ["2-4", "1-3", "2-12", "1-10", "1-30"]
    industryFirstArr = ["101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111", "112", "113",
                            "114", "115", "116", "117", "118", "119", "120", "121", "122", "123", "124", "125", "126",
                            "127", "128", "129", "999"]

    label = float(parseArr[1])
    featureMap = json.loads(parseArr[15])

    posDisplay = float(featureMap["posDisplay2"])
    posClick = float(featureMap["posClick2"])
    userDisplay = float(featureMap["userDisplay"])
    userClick = float(featureMap["userClick"])
    slotId = featureMap["slotId"]
    sex = featureMap["sex"]
    industryFirst = featureMap["industryFirst"]
    slotIdIdx = slotArr.index(slotId)
    slotIdSize = len(slotId)
    sexIdx = sexArr.index(sex)
    industryFirstIdx = industryFirstArr.index(industryFirst) if industryFirst in industryFirstArr else -1
    industryFirstSize = len(industryFirstArr)+1

    posDisplayIdx,posCtrIdx = getDisplayCtrIdx(posDisplaySize, posCtrSize, posDisplayUnit, posCtrUnit, posDisplay,posClick)
    userDisplayIdx,userCtrIdx = getDisplayCtrIdx(userDisplaySize, userCtrSize, userDisplayUnit, userCtrUnit, userDisplay,userClick)

    posDisplayCtrIdx = calculateCombFeatureWeightIdx(posDisplayIdx, posCtrIdx, posCtrSize)
    posDisplayCtrSize  = posDisplaySize * posCtrSize

    userDisplayCtrIdx = calculateCombFeatureWeightIdx(userDisplayIdx, userCtrIdx, userCtrSize)
    userDisplayCtrSize = userDisplaySize * userCtrSize

    sexIndustryFirstIdx = calculateCombFeatureWeightIdx(sexIdx, industryFirstIdx, industryFirstSize)
    if industryFirstIdx == -1:
        sexIndustryFirstIdx = len(sexArr) * industryFirstSize - 1
    sexIndustryFirstSize = len(sexArr) * industryFirstSize

    featureVector = mkFeatureVector([(posDisplayCtrIdx, posDisplayCtrSize), (userDisplayCtrIdx, userDisplayCtrSize),
                                     (sexIndustryFirstIdx, sexIndustryFirstSize), (slotIdIdx, slotIdSize)])

    return LabeledPoint(label, featureVector)

rawTrainingData = sc.textFile("tracelog/parse-/2016-09-01/")
recordsTrain = rawTrainingData.map(lambda line: line.split("|"))
trainingData = recordsTrain.filter(lambda x: len(x)==20 and x[8] == "1").map(featureExtraction)

print trainingData.take(10)

trainingData.cache()
metric_trainingDataCount = trainingData.count()

model = LogisticRegressionWithLBFGS.train(trainingData, regType="l1", intercept=True)
model.clearThreshold()
model.save(sc, "training_pipeline/model/lr0927pytest")
metric_intercept = model.intercept
metric_weights = model.weights
metric_featureCount = model.numFeatures

predictionsAndLabels = trainingData.map (lambda x:(model.predict(x.features), x.label))
metric_trainAuc = BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC
metric_trainMse = predictionsAndLabels.map( lambda x,y : (x - y)*(x-y)).mean()

print metric_weights, metric_intercept, metric_featureCount, metric_trainAuc, metric_trainMse