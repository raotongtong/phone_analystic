package com.phone.analystic.mr.nu;

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
 * @ClassName: NewUserReducer
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 2:56
 * @Version: 1.0
 * @Description: 用户模块下的新增用户的reducer
 * 问题：
 * (1)这里输出的StatsUserDimension、OutputWritable都是对象，输出到数据库的不可能是对象，而是对象中所对应维度的信息,所以我们需要
 *      创建一个方法，将对象转化成所对应的维度的id
 */
public class NewUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,OutputWritable>{
    private static final Logger logger = Logger.getLogger(NewUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet(); //用于去重uuid
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        map.clear();

        /**
         * 循环
         * 这里说明OutputWritable里面的内容依赖TimeOutputValue，也就是穿的参数都从map传入的，
         * 包括传入的kpi(如：new_user)也是从map端传入的
         */
        for(TimeOutputValue tv : values){
            this.unique.add(tv.getId());
        }

        //构造输出的value
//        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));
        if(key.getStatsCommonDimention().getKpiDimension().getKpiName().equals(KpiType.NEW_USER.kpiName)){
            this.v.setKpi(KpiType.NEW_USER);
        }

        //new IntWritable(-1)，这个只是个标识，-1 是key，新增用户是value，说明通过-1就可以找到新增用户
        //这个是给NewUserOutputWritter用的，也就是给sql语句赋值用的
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
    }
}
