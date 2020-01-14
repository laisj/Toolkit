package com.xiaomi.miui.ad.predict.feature;

import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.thrift.model.*;
import com.xiaomi.miui.ad.predict.util.FeatureCollector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Null;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created: wwxu(xuwenwen@xiaomi.com)
 * Date: 2017-09-13
 */
public class AdInfoTagCT extends BaseFeature {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureManager.class);

    private static void put2Map(String val, String key, Map<String, List<String>> map) {
        if (StringUtils.isBlank(val)) {
            return;
        }
        List<String> list = new ArrayList<>();
        String[] segs = val.split(",");
        for (int i = 0 ; i < segs.length; i++) {
            list.add(segs[i]);
        }
        map.put(key, list);
    }

    private static void putActivate2Map( Map<Integer, ActStat> activateMap, String key, Map<String, List<String>> adMap) {
        if (activateMap == null || activateMap.size() == 0) {
            return;
        }
        for(Map.Entry<Integer, ActStat> entry : activateMap.entrySet()) {
            String daySpan = String.valueOf(entry.getKey());
            ActStat actStat = entry.getValue();
            Double sdBucket = Math.log10(actStat.getStartDownloadCount());
            String sdBucketStr = String.valueOf(sdBucket.intValue() + 1);
            String actRateStr = "0";
            if (actStat.getAppUsageCount() > 0) {
                Double actRate = actStat.getAppUsageCount() * 1.0 / actStat.getStartDownloadCount();
                Double logRate = Math.log(actRate * 1000);
                actRateStr = String.valueOf(logRate.intValue() + 1);
            }
            //put2Map(sdBucketStr+"_"+actRateStr, key+ "_" + daySpan, adMap);
            put2Map(actRateStr, key+ "_" + daySpan, adMap);
        }
    }

    public static Map<String, List<String>> genAdMap(AdData adData) {
        // ad map string, list string
        // adid, adid, adid, adid, an admap is all features for an ad
        Map<String, List<String>> adMap = new HashMap<String, List<String>>();
        // get id or get id list, put adid list into admap, name is adid
        put2Map(String.valueOf(adData.getId()), "adid", adMap);

        // set asset, asset? idea? sth between ad and campaign?
        if(adData.isSetAssetId()) {
            put2Map(String.valueOf(adData.getAssetId()), "adtz", adMap);
        }
        if(adData.isSetCampaignId()){
            put2Map(String.valueOf(adData.getCampaignId()), "adcp", adMap);
        }
        if(adData.isSetAppInfo()) {
            AppInfo appInfo = adData.getAppInfo();
            if (appInfo.isSetAppId()) {
            // ad app id, sth large than ad
                put2Map(String.valueOf(appInfo.getAppId()), "adap", adMap);
            }
            // app category id, large than app
            if (appInfo.isSetLevel1CategoryId()) {
                put2Map(String.valueOf(appInfo.getLevel1CategoryId()), "adc1", adMap);
            }
            // etc.
            if(appInfo.isSetLevel2CategoryId()){
                put2Map(String.valueOf(appInfo.getLevel2CategoryId()), "adc2", adMap);
            }
            // display name, nlp?
            if(appInfo.isSetDisplayName()) {
                put2Map(appInfo.getDisplayName(), "adnm", adMap);
            }
            // all put into map can be blank
            if (appInfo.isSetAppActivateData()) {
                // app activate data seems not useful
                AppActivateData appActivateData = appInfo.getAppActivateData();
//                putActivate2Map(appActivateData.getAppActivateMap(), "adaa", adMap);
//                putActivate2Map(appActivateData.getC1ActivateMap(), "ad1a", adMap);
//                putActivate2Map(appActivateData.getC2ActivateMap(), "ad2a", adMap);
            }
        }
        return adMap;
    }


    public FeatureCollector collect(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, FeatureCollector collector) {
        // what do collect do? feature extraction
        if (adData == null) {
            LOGGER.info("AdInfo null");
            return collector;
        }
        LOGGER.debug("AdInfo feature extraction start");
        // put ad info into feature list
        Map<String, List<String>> adMap = genAdMap(adData);
        // tagId, tag for app, seems not useful
        String tagId = contextData.getTagId();
        StringBuilder feature = new StringBuilder(64);
        // put all feature into string, "key.value"
        for (Map.Entry<String, List<String>> ad : adMap.entrySet()) {
            for (String adVal : ad.getValue()) {
                feature.append(ad.getKey()).append(".").append(adVal);
          //              .append(".").append(tagId);
                collector.add(feature.toString());
                feature.setLength(0);
            }
        }

        LOGGER.debug("AdInfo feature extraction end");
        return collector;
    }
}
