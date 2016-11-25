// display 2-4 cpm 头广
// schema
/*
| 1 | <timestamp> | 时间格式“yyyy-MM-dd HH:mm:ss.S” |
| 2 | <customMomoId> | 广告主用户id |
| 3 | <parentTraceId> | 父级别的traceId |
| 4 | <traceId> | traceId，跟踪记录时使用 |
| 5 | <returnTime> | 广告下发时间戳：long毫秒格式 |
| 6 | <adId> | 返回广告id |
| 7 | <fee> | 千次曝光费用 |
| 8 | <budgetType> | 预算类型：1.总预算 0.天预算 |
| 9 | <customMomoId> | 广告主用户id |
| 10 | <userMomoId> | 被展示广告的用户id |
| 11 | <slotId> | 广告位的id：1-3 1-8 1-23 2-4 3-3 |
| 12 | <adType> | 广告类型：：1.到店通商家 2.品牌 3.应用 4.公益 5.大众点评 6.百度 7.群组 8.品友 9.多盟 10.InMobi |
| 13 | <feeType> | 计费类型：0.CPM 1.CPC |
| 14 | <campaignId> | 创意id |
| 15 | <ideaId> | 素材id |
| 16 | <logType> | 固定字符串填充为"display" |
| 17 | <bucketCode> | （CTR）流量标签，目前以陌陌用户id最后2位数生成，即总计有0-99个不同的值 |
| 18 | <bucketExpName> | （CTR）实验名称 |
| 19 | <bucketCtrModelName> | （CTR）ctr模型名称 |
| 20 | <bucketBidModelName> | （CTR）bid模型名称 |
| 21 | <bucketCtr> | （CTR）点击率 |
| 22 | <bucketModelStr> | （CTR）模型冗余配置 |
| 23 | <svip> | 是否是svip曝光请求，1是；0或者空不是 |
| 24 | <interests> | 感兴趣 |
| 25 | <os> | 操作系统 |
| 26 | <interVersion> | 客户端版本 |
*/

import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

val data = sc.textFile("log/2016-06-0[2-6]/*/08/display-*")
// val data = sc.textFile("rtlog/2016-07-18/08/*/home/logs/adengine/display")
// 最新最旧数据
data.top(1)
data.takeOrdered(1)
val data2 = data.map(x=>x.split("\\|")).filter(x=>x(10)=="2-4")
// 过滤时间
// val data2 = data.map(x=>x.split("\\|")).filter(x=>x(10)=="2-4").filter(x=>x(0)<"2016-07-17 18:10")
// cpc
val data3 = data2.filter(x=>x(12)=="0")
// 广告id
val data4 = data3.map(x=>(x(5),1)).reduceByKey(_+_).sortBy(-_._2)
// 广告主id
val data5 = data3.map(x=>(x(8),1)).reduceByKey(_+_).sortBy(-_._2)

for (clock <- Array("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")) {
  var fileName = "rtlog/2016-07-18/" + clock + "/*/home/logs/adengine/display"
  var data = sc.textFile(fileName)
  // ...
}

// cache未命中
data2.count
data2.filter(x=>x(20)=="0.015").count
// lr feature未传到
data2.filter(x=>x(17)=="exp20" && x(20)=="0.005").count


