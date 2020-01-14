package com.xiaomi.miui.ad.predict.feature;

import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.thrift.model.AdData;
import com.xiaomi.miui.ad.predict.thrift.model.ContextData;
import com.xiaomi.miui.ad.predict.thrift.model.EnvironmentData;
import com.xiaomi.miui.ad.predict.thrift.model.UserData;
import com.xiaomi.miui.ad.predict.util.FeatureCollector;
import org.apache.commons.lang.StringUtils;
import org.omg.CORBA.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created: wwxu(xuwenwen@xiaomi.com)
 * Date: 2017-09-13
 */
public class ContextInfoTagCT extends BaseFeature {
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

    public static Map<String, List<String>> genContextMap(ContextData contextData) {
        Map<String, List<String>> contextMap = new HashMap<String, List<String>>();
        put2Map(contextData.getTagId(), "cotg", contextMap);

        String ts = String.valueOf(contextData.getTimestamp());
        if (ts.length() == 6) {
            String hod = ts.substring(2, 4);
            put2Map(hod, "cohd", contextMap);
        }

        if (contextData.isSetEnvironmentData()) {
            EnvironmentData env = contextData.getEnvironmentData();
            put2Map(env.getConnectionType(), "coct", contextMap);
            put2Map(env.getIp(), "coip", contextMap);
        }

        if (contextData.isSetDeviceData()) {
            put2Map(contextData.getDeviceData().getAndroidVersion(), "coav", contextMap);
            put2Map(contextData.getDeviceData().getMiuiVersion(), "comi", contextMap);
        }

        return contextMap;
    }


    public FeatureCollector collect(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, FeatureCollector collector) {
        if (contextData == null) {
            LOGGER.warn("context data null");
            return collector;
        }
        LOGGER.debug("Context feature extraction start");
        StringBuilder feature = new StringBuilder(64);
        String tagId = contextData.getTagId();
        Map<String, List<String>> contextMap = genContextMap(contextData);

        for (Map.Entry<String, List<String>> context : contextMap.entrySet()) {
            for (String contextVal : context.getValue()) {
                feature.append(context.getKey()).append(".").append(contextVal);
  //                      .append(".").append(tagId);
                collector.add(feature.toString());
                feature.setLength(0);
            }
        }

        LOGGER.debug("Context feature extraction end");
        return collector;
    }
}
