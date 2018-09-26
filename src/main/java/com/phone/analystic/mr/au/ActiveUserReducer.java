package com.phone.analystic.mr.au;

import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * 活跃用户reducer方法
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,OutputWritable>{
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet(); //用于去重uuid
    //mapWritable用来存储去重后的newuser，mapWritable是一种map的优化
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        map.clear();
        unique.clear();

        /**
         * 循环
         * 这里说明OutputWritable里面的内容依赖TimeOutputValue，也就是传的参数都从map传入的，
         * 包括传入的kpi(如：new_user)也是从map端传入的
         */
        for(TimeOutputValue tv : values){
            unique.add(tv.getId());
        }

        //构造输出的value
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));

        //new IntWritable(-1)，这个只是个标识，-1 是key，新增用户是value，说明通过-1就可以找到新增用户
        //这个是给NewUserOutputWritter用的，也就是给sql语句赋值用的
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
    }
}
