package com.phone.analystic.mr.au;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.DateEnum;
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
public class ActiveUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,OutputWritable>{
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet(); //用于去重uuid
    //mapWritable用来存储去重后的newuser，mapWritable是一种map的优化
    private MapWritable map = new MapWritable();

    //按小时统计
    private Map<Integer,Set<String>> hourlyMap = new HashMap<>();
    private MapWritable hourlyWritable = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化按小时的容器
        for(int i=0;i<24;i++){
            this.hourlyMap.put(i,new HashSet<String>());
            this.hourlyWritable.put(new IntWritable(i),new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            for(TimeOutputValue tv : values){
                unique.add(tv.getId());

                //统计小时的活跃用户
                if(key.getStatsCommonDimention().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)){
                    int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                    this.hourlyMap.get(hour).add(tv.getId());
                }
            }

            //按小时统计
            if(key.getStatsCommonDimention().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)){
                for (Map.Entry<Integer,Set<String>> en : hourlyMap.entrySet()){
                    //构造输出的value
                    this.hourlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
                }
                this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
                this.v.setValue(this.hourlyWritable);
                context.write(key,this.v);
            }

            //构造输出的value
            this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));

            //new IntWritable(-1)，这个只是个标识，-1 是key，新增用户是value，说明通过-1就可以找到新增用户
            //这个是给NewUserOutputWritter用的，也就是给sql语句赋值用的
            this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.v.setValue(this.map);
            //输出
            context.write(key,this.v);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            this.unique.clear();
            this.hourlyMap.clear();
            this.hourlyWritable.clear();
            for(int i = 0; i < 24 ; i++){
                this.hourlyMap.put(i,new HashSet<String>());
                this.hourlyWritable.put(new IntWritable(i),new IntWritable(0));
            }
        }
    }
}
