import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats

val clickData = sc.textFile("rtlog/2016-08-14/*/*/adengine/click/")
clickData.cache()
clickData.count
val displayData = sc.textFile("rtlog/2016-08-14/*/*/adengine/display/")
displayData.cache()

//val adSet = Array("201606141451995037",	"2016080209351068991",	"2016081112551080805",	"2016070414361026295")
val adSet = Array("201606141451995037","2016080819131077345","2016071914381051358","2016081017421079933","2016081018391080025")

// val adSet = Array("2016080209351068991","2016071914381051358","2016081018291080009")
val slotSet = Array("2-4","2-12","1-3","1-10","1-30")
//"2016081017421079933","2016072511321059330"
var pctrIosArr = Array[AnyVal]()
var ctrIosArr = Array[AnyVal]()
var pctrAdrArr = Array[AnyVal]()
var ctrAdrArr = Array[AnyVal]()
var ctrArr = Array[AnyVal]()

for (adId <- adSet) {
  for (slot <- slotSet) {
    val adDisplay = displayData.filter(x => x.split("\\|")(5) == adId && x.split("\\|")(10)==slot)
    val adDisplayIos = adDisplay.filter(x => x.split("\\|")(24) == "1")
    val adDisplayAdr = adDisplay.filter(x => x.split("\\|")(24) == "2")

    val adClick = clickData.filter(x => x.split("\\|")(5) == adId && x.split("\\|")(10)==slot)
    val adClickIos = adClick.filter(x => x.split("\\|")(24) == "1")
    val adClickAdr = adClick.filter(x => x.split("\\|")(24) == "2")

    val pctrIos = adDisplayIos.map(x => x.split("\\|")(20).toDouble).mean
    val ctrIos = adClickIos.count().toDouble / adDisplayIos.count()
    val pctrAdr = adDisplayAdr.map(x => x.split("\\|")(20).toDouble).mean
    val ctrAdr = adClickAdr.count().toDouble / adDisplayAdr.count()
    val ctr = adClick.count().toDouble / adDisplay.count()

    pctrIosArr = pctrIosArr :+ pctrIos
    ctrIosArr = ctrIosArr :+ ctrIos
    pctrAdrArr = pctrAdrArr :+ pctrAdr
    ctrAdrArr = ctrAdrArr :+ ctrAdr
    ctrArr = ctrArr :+ ctr
  }
}

println(pctrIosArr.mkString("|"))
println(ctrIosArr.mkString("|"))
println(pctrAdrArr.mkString("|"))
println(ctrAdrArr.mkString("|"))
println(ctrArr.mkString("|"))



/* 8.15 pos ad
scala> println(pctrIosArr.mkString("|"))
0.00592468625122135|0.006362627043134589|0.0064288738519820005|0.006614502956265668|0.009416692839702702|0.009088426022356443|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0

scala> println(ctrIosArr.mkString("|"))
0.00524348075381618|0.006158210680558072|0.00794048375640211|0.007545954402190167|0.01411192737520302|0.01259459834756648|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN

scala> println(pctrAdrArr.mkString("|"))
0.005783900860207786|0.0064976972180500235|0.006785742376544971|0.006747218626280399|0.00972979752408375|0.009482845841720627|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0

scala> println(ctrAdrArr.mkString("|"))
0.0076818285926500895|0.008011788925065226|0.009034492922571163|0.009202119860072477|0.012037907773547702|0.01171107969668233|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN

scala> println(ctrArr.mkString("|"))
0.0071311808544204285|0.007567328926150281|0.00880438127400629|0.008846791084435274|0.012378799758505563|0.011889967892307087|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN

8.16 pos ad
scala> println(pctrIosArr.mkString("|"))
0.004689322321301252|0.006184780917433574|0.007118890984788471|0.0|0.007082703374744515|0.007528906620482054|0.009285029871323526|0.0|0.006583822760399546|0.0|0.0|0.0058157952095968714|0.007971242480715249|0.0|0.0|0.00746416681665081|0.008000388430477526|0.0|0.0|0.008272252502870829

scala> println(ctrIosArr.mkString("|"))
0.0017868150015410021|0.004612776385696828|0.0053647197274887875|NaN|0.002326435655687523|0.00824725263396631|0.010225183823529412|NaN|0.008080014650818482|NaN|NaN|0.009165762943326505|0.012063881484277323|NaN|NaN|0.01340086905098604|0.012750175561797753|NaN|NaN|0.01547008685244788

scala> println(pctrAdrArr.mkString("|"))
0.0|0.006143467405171257|0.0070251752016146475|0.0|0.0|0.007524622453527698|0.00994738672230246|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0|0.0

scala> println(ctrAdrArr.mkString("|"))
NaN|0.007343942383290561|0.007110702155819622|NaN|NaN|0.008888073202066018|0.010966672016262773|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN|NaN

scala> println(ctrArr.mkString("|"))
0.0017868150015410021|0.00673651669313739|0.006779387985406652|NaN|0.002326435655687523|0.008733494143684878|0.010826643523540899|NaN|0.008080014650818482|NaN|NaN|0.009165762943326505|0.012063881484277323|NaN|NaN|0.01340086905098604|0.012750175561797753|NaN|NaN|0.01547008685244788

8.14
scala> println(pctrIosArr.mkString("|"))
0.0|0.0|0.006614020892664644|0.007349028009739585|0.007239173661630291|0.0|0.0|0.0|0.0|0.0|0.005891776267511512|0.009777708735458966|0.0|0.0|0.0|0.009236360558839335|0.01475498914550257|0.0|0.0|0.0|0.005710135574933452|0.009754042130951908|0.0|0.0|0.0

scala> println(ctrIosArr.mkString("|"))
NaN|NaN|0.008077102540586635|0.01054934179080138|0.010986756202201083|NaN|NaN|NaN|NaN|NaN|0.005260057805843595|0.014120415767797607|NaN|NaN|NaN|0.006778613648576034|0.014289575323237344|NaN|NaN|NaN|0.0053135850996740425|0.015111897770089974|NaN|NaN|NaN

scala> println(pctrAdrArr.mkString("|"))
0.0|0.0|0.0|0.0|0.0|0.0046953929591414855|0.004989540471564482|0.015922144522144525|0.025437623762376235|0.017376237623762378|0.006134384888245187|0.009588336682795363|0.0|0.0|0.0|0.00950627411939613|0.014745026813112898|0.0|0.0|0.0|0.00590734661455814|0.009599822286101673|0.0|0.0|0.0

scala> println(ctrAdrArr.mkString("|"))
NaN|NaN|NaN|NaN|NaN|0.003951682982710881|0.003273685424558851|0.0|0.0|0.0|0.0077012016250782465|0.012226929674673051|NaN|NaN|NaN|0.012807343228876465|0.019500809697959642|NaN|NaN|NaN|0.007566848067434929|0.013282520846866907|NaN|NaN|NaN

scala> println(ctrArr.mkString("|"))
NaN|NaN|0.008077102540586635|0.01054934179080138|0.010986756202201083|0.003951682982710881|0.003273685424558851|0.0|0.0|0.0|0.007211581861698077|0.012490881124395132|NaN|NaN|NaN|0.011816943960770553|0.018527743590745407|NaN|NaN|NaN|0.007145351186762197|0.013495848513752287|NaN|NaN|NaN
*/