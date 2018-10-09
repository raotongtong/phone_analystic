package com.phone.analystic.mr.location;

import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.LocationOutputValue;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.LocationOutputWritable;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 活跃用户reducer方法
 */
public class LocationReducer extends Reducer<StatsLocationDimension,LocationOutputValue,
        StatsLocationDimension,LocationOutputWritable>{
    private static final Logger logger = Logger.getLogger(LocationReducer.class);
    private LocationOutputWritable v = new LocationOutputWritable();
    private Set unique = new HashSet(); //用于去重uuid
    private Map<String,Integer> map = new HashMap<>();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<LocationOutputValue> values, Context context) throws IOException, InterruptedException {
        unique.clear();
        map.clear();

        for(LocationOutputValue lv : values){
            unique.add(lv.getUuid()); //用于去重uuid

            if(map.containsKey(lv.getSessionId())){
                map.put(lv.getSessionId(),2);
            }
            map.put(lv.getSessionId(),1);
        }

        int bounceSession = 0;
        for(Map.Entry<String,Integer> en : map.entrySet()){
            if(en.getValue() == 1){
                bounceSession ++;
            }
        }

        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));

        this.v.setAus(unique.size());
        this.v.setSessions(map.size());
        this.v.setBounceSession(bounceSession);

        context.write(key,v);
    }
}
