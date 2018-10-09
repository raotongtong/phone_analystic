package com.phone.analystic.mr.session;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * 活跃用户reducer方法
 */
public class SessionReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,OutputWritable>{
    private static final Logger logger = Logger.getLogger(SessionReducer.class);
    private OutputWritable v = new OutputWritable();
    private Map<String,List<Long>> map = new HashMap<>();
    //mapWritable用来存储去重后的newuser，mapWritable是一种map的优化
    private MapWritable mapWritable = new MapWritable();

    //按小时统计session
    private Map<Integer,Set<String>> hourlySessionMap = new HashMap<>();
    //按小时统计sessionTime
    private Map<Integer,List<Long>> hourlySessionTimeMap = new HashMap<>();

    private MapWritable hourlyWritable = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化按小时的容器
        for(int i=0;i<24;i++){
            this.hourlySessionMap.put(i,new HashSet<String>());
            this.hourlySessionTimeMap.put(i,new ArrayList<Long>());
            this.hourlyWritable.put(new IntWritable(i),new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        try {
            for(TimeOutputValue tv : values){
                if(map.containsKey(tv.getId())){
                    map.get(tv.getId()).add(tv.getTime());
                }else{
                    List<Long> li = new ArrayList<>();
                    li.add(tv.getTime());
                    map.put(tv.getId(),li);
                }

                //统计小时的session和sessionTime
                if(key.getStatsCommonDimention().getKpiDimension().getKpiName().equals(KpiType.SESSION.kpiName)){
                    int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                    //统计session
                    this.hourlySessionMap.get(hour).add(tv.getId());
                    //统计sessionTime
                    this.hourlySessionTimeMap.get(hour).add(tv.getTime());
                }
            }

            //按小时统计
            if(key.getStatsCommonDimention().getKpiDimension().getKpiName().equals(KpiType.SESSION.kpiName)){
                //按小时统计session
                for (Map.Entry<Integer,Set<String>> en : hourlySessionMap.entrySet()){
                    //构造输出的value
                    this.hourlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
                }
                this.v.setKpi(KpiType.HOURLY_SESSION);
                this.v.setValue(this.hourlyWritable);
                context.write(key,this.v);

                //按小时统计sessionLength
                for (Map.Entry<Integer,List<Long>> en : hourlySessionTimeMap.entrySet()){
                    Collections.sort(en.getValue());
                    int sessionLength = (int) (en.getValue().get(en.getValue().size()-1) - en.getValue().get(0));
                    if (sessionLength > 0 && sessionLength <= GlobalConstants.DAY_OF_MILISECONDS) {
                        //不足一秒算一秒
                        if (sessionLength % 1000 == 0) {
                            sessionLength = sessionLength / 1000;
                        } else {
                            sessionLength = sessionLength / 1000 + 1;
                        }
                    }
                    //构造输出的value
                    this.hourlyWritable.put(new IntWritable(en.getKey()),new IntWritable(sessionLength));
                }
                this.v.setKpi(KpiType.HOURLY_SESSION_LENGTH);
                this.v.setValue(this.hourlyWritable);
                context.write(key,this.v);

            }

            int sessionTime = 0;
            int session = 0;
            if(map != null){
                for(Map.Entry<String,List<Long>> en : map.entrySet()){
                    session++;
                    List<Long> list = en.getValue();
                    Collections.sort(list);
                    int singleSessionTime = (int)(list.get(list.size() - 1) - list.get(0));
                    sessionTime += singleSessionTime;
                }
            }

            if (sessionTime > 0 && sessionTime <= GlobalConstants.DAY_OF_MILISECONDS) {
                //不足一秒算一秒
                if (sessionTime % 1000 == 0) {
                    sessionTime = sessionTime / 1000;
                } else {
                    sessionTime = sessionTime / 1000 + 1;
                }
            }

            //构造输出的value
            this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));

            //new IntWritable(-1)，这个只是个标识，-1 是key，新增用户是value，说明通过-1就可以找到新增用户
            //这个是给NewUserOutputWritter用的，也就是给sql语句赋值用的
            this.mapWritable.put(new IntWritable(-1),new IntWritable(session));
            this.mapWritable.put(new IntWritable(-2),new IntWritable(sessionTime));
            this.v.setValue(this.mapWritable);
            //输出
            context.write(key,this.v);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            map.clear();
            mapWritable.clear();
            hourlySessionMap.clear();
            hourlySessionTimeMap.clear();
            hourlyWritable.clear();
        }
    }
}
