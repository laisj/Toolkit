package com.xiaomi.miui.ad.predict.feature;

import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.thrift.model.*;
import com.xiaomi.miui.ad.predict.util.FeatureCollector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created: wwxu(xuwenwen@xiaomi.com)
 * Date: 2017-09-14
 */
public class AXUTagCT extends BaseFeature {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureManager.class);
    private static final int maxAppCount = 80;

    private static final Set<String> adFeaSet = new HashSet<String>() {
        {
            add("adid");
            add("adtz");
            add("adc1");
      //      add("adc2");
            add("adap");
            add("adcp");
      //      add("adaa");
      //      add("ad1a");
      //      add("ad2a");
        }
    };
    private static final Set<String> userFeaSet = new HashSet<String>() {
        {
            add("usag");
            add("usgd");
            add("usdi");
            add("usia");
            add("usin");
            add("usua");
//            add("usuo");
//            add("usut");
//            add("usun");
            add("usai");
            add("usau");
            add("usas");
            add("usa1");
            add("usa2");
            add("usa3");
            add("usrt");
//            add("usrs");
//            add("usru");
      //      add("usqa");
   //         add("usq1");
         //   add("usq2");
        }
    };

    private static final Set<String> conFeaSet = new HashSet<String>() {
        {
            add("cohd");
            add("coct");
            add("coav");
            add("cotg");
            add("comi");
 //           add("coip");
        }
    };

    public FeatureCollector collect(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, FeatureCollector collector) {
        LOGGER.debug("Ad_X_User feature extraction start");
        if (userData == null || adData == null) {
            LOGGER.warn("one null, ad {}, user data {}", adData, userData);
            return collector;
        }

// adMap, feature about ad
        Map<String, List<String>> adMap = AdInfoTagCT.genAdMap(adData);
        // feature about user
        Map<String, List<String>> userMap = UserInfoTagCT.genUserMap(userData);
        Map<String, List<String>> conMap = ContextInfoTagCT.genContextMap(contextData);

// super feature
        List<String> supFea = new ArrayList<>();

        StringBuilder feature = new StringBuilder(64);

        // ad x user
// ad multiply user?
        for (Map.Entry<String, List<String>> user : userMap.entrySet()) {
        // user have a key which is not in target set?
            if (!userFeaSet.contains(user.getKey().split("_")[0]))
                continue;
            for (Map.Entry<String, List<String>> ad : adMap.entrySet()) {
                if (!adFeaSet.contains(ad.getKey().split("_")[0]))
                    continue;
                if (user.getKey().equals("usia") && ad.getKey().equals("adap")) {
                    if (user.getValue().contains(adMap.get("adap").get(0))) {
                        supFea.add("supa");
                        feature.append("supa");
                        collector.add(feature.toString());
                        feature.setLength(0);
                    }
                } else {
                    for (String adVal : ad.getValue()) {
                        for (String userVal : user.getValue()) {
                            feature.append(ad.getKey()).append("&").append(user.getKey())
                                    .append(".")
                                    .append(adVal).append("&").append(userVal);

                            collector.add(feature.toString());
                            feature.setLength(0);
                        }
                    }
                }
            }
        }

        // context x user
        for (Map.Entry<String, List<String>> co : conMap.entrySet()) {
            if (!conFeaSet.contains(co.getKey().split("_")[0]))
                continue;
            for (Map.Entry<String, List<String>> user : userMap.entrySet()) {
                    if (!userFeaSet.contains(user.getKey().split("_")[0]))
                        continue;
                    for (String coVal : co.getValue()) {
                        for (String userVal : user.getValue()) {
                            feature.append(co.getKey()).append("&").append(user.getKey())
                                .append(".")
                                .append(coVal).append("&").append(userVal);
                        //  .append("&").append(tagId);
                        collector.add(feature.toString());
                        feature.setLength(0);
                    }
                }
            }
        }

        // ad x context
        for (Map.Entry<String, List<String>> ad : adMap.entrySet()) {
            if (!adFeaSet.contains(ad.getKey().split("_")[0]))
                continue;
            for (Map.Entry<String, List<String>> co : conMap.entrySet()) {
                if (!conFeaSet.contains(co.getKey().split("_")[0]))
                    continue;
                for (String adVal : ad.getValue()) {
                    for (String coVal : co.getValue()) {
                        feature.append(ad.getKey()).append("&").append(co.getKey())
                                .append(".")
                                .append(adVal).append("&").append(coVal);
                        //.append("&").append(tagId);
                        collector.add(feature.toString());
                        feature.setLength(0);

                    }
                }
            }
        }

        // ad x user x context
        for (Map.Entry<String, List<String>> ad : adMap.entrySet()) {
            if (!adFeaSet.contains(ad.getKey().split("_")[0]))
                continue;
            for (Map.Entry<String, List<String>> user : userMap.entrySet()) {
                if (!userFeaSet.contains(user.getKey().split("_")[0])
                        || ad.equals("adap") && user.equals("usia")) {
                    continue;
                }
                for (Map.Entry<String, List<String>> co : conMap.entrySet()) {
                    if (!conFeaSet.contains(co.getKey().split("_")[0]))
                        continue;

//                    if (ad.getKey().equals("adap") && user.getKey().equals("usia") && co.getKey().equals("comi")) {
//                        if (user.getValue().contains(ad.getValue().get(0))) {
//                            for (String coVal : co.getValue()) {
//                                feature.append("supa&").append("comi.").append(coVal);
//                                collector.add(feature.toString());
//                                feature.setLength(0);
//                            }
//                        }
//                    }

                    for (String adVal : ad.getValue()) {
                        for (String userVal : user.getValue()) {
                            for (String coVal : co.getValue()) {
                                feature.append(ad.getKey()).append("&").append(user.getKey()).append("&").append(co.getKey())
                                        .append(".")
                                        .append(adVal).append("&").append(userVal).append("&").append(coVal);
                                //         .append("&").append(tagId);
                                collector.add(feature.toString());
                                feature.setLength(0);

                            }
                        }
                    }
                }
            }
        }

        //contex x sup
        for (Map.Entry<String, List<String>> co : conMap.entrySet()) {
            if (!conFeaSet.contains(co.getKey().split("_")[0]))
                continue;
            for (String sup : supFea) {
                for (String coVal : co.getValue()) {
                    feature.append(sup).append("&").append(co.getKey())
                            .append(".")
                            .append(coVal);

                    collector.add(feature.toString());
                    feature.setLength(0);
                }
            }

        }


        LOGGER.debug("Ad_X_User feature extraction end");
        return collector;
    }
}
