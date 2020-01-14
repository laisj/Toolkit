package com.xiaomi.contest.cvr.utils

import com.xiaomi.contest.cvr.CVRInstance
import com.xiaomi.contest.cvr.group.FeaGroup
import com.xiaomi.contest.cvr.samples.{BaseFea, Sample, FeaType}

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object FeaUtils {
  final private val groupSeparator = '#'

  // for continuous features which has multiple value
  def makeFea(group: FeaGroup, feature: String, value: Double): BaseFea = {
    val ans = new BaseFea()
    ans.setType(FeaType.Continuous)
    ans.setGroup_id(group.ordinal)
    ans.setGroup_name(group.name)
    val fea = group.name + groupSeparator + feature
    ans.setIdentifier(FeatureHash.hashToLong(fea))
    ans.setFea(fea)
    ans.setValue(value)
    ans
  }

  // for continuous features which has single value
  def makeFea(group: FeaGroup, value: Double): BaseFea = {
    val ans = new BaseFea()
    ans.setType(FeaType.Continuous)
    ans.setGroup_id(group.ordinal)
    ans.setGroup_name(group.name)
    val fea = group.name + groupSeparator + group.name
    ans.setIdentifier(FeatureHash.hashToLong(fea))
    ans.setFea(fea)
    ans.setValue(value)
    ans
  }

  // for categorical features
  def makeFea(group: FeaGroup, feature: String): BaseFea = {
    val ans = new BaseFea()
    ans.setType(FeaType.Categorical)
    ans.setGroup_id(group.ordinal)
    ans.setGroup_name(group.name)
    val fea = group.name + groupSeparator + feature
    ans.setIdentifier(FeatureHash.hashToLong(fea))
    ans.setFea(fea)
    ans.setValue(1.0) // value always 1.0
    ans
  }

  def makeSample(features: List[BaseFea], instance: CVRInstance): Sample = {
    val ans = new Sample()
    ans.setLabel(instance.getData.getLabel)
    ans.setInstance_id(instance.getData.getInstance_id)
    ans.setGroup_number(FeaGroup.values().length)
    ans.setFeatures(features)
    ans.setTime(instance.getData.getClick_time)
    ans
  }
}
