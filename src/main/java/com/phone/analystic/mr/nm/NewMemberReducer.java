package com.phone.analystic.mr.nm;

import com.phone.Util.JdbcUtil;
import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * 活跃用户reducer方法
 */
public class NewMemberReducer extends Reducer<StatsUserDimension,TimeOutputValue,
        StatsUserDimension,OutputWritable>{
    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private OutputWritable v = new OutputWritable();
    Connection conn = null;
    PreparedStatement ps = null;
    private Set unique = new HashSet(); //用于去重uuid
    //mapWritable用来存储去重后的newuser，mapWritable是一种map的优化
    private MapWritable map = new MapWritable();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        map.clear();
        unique.clear();

        for(TimeOutputValue tv : values){
            String memberId = tv.getId();
            if(StringUtils.isNotEmpty(memberId)) {
                try {
                    ps = conn.prepareStatement("select * from member_info where member_id = ?");
                    ps.setString(1, memberId);
                    Date date = key.getStatsCommonDimention().getDateDimension().getCalendar();
                    if (ps.execute()) {
                        //如果查询的到就设置最新的登录时间
                        ps = conn.prepareStatement("UPDATE member_info SET last_visit_date = ?,created=? WHERE member_id = ?");
                        ps.setString(1, TimeUtil.parseLong2String(date.getTime()));
                        ps.setString(2,TimeUtil.parseLong2String(date.getTime()));
                        ps.setString(3, memberId);
                        ps.execute();
                    } else {
                        //如果查询不到，则把memberId加到一个map中，并且给其添加第一次登录时间和最近访问时间
                        unique.add(memberId);
                        ps = conn.prepareStatement("INSERT INTO member_info VALUES (?,?,?,?)");
                        ps.setString(1, memberId);
                        ps.setString(2, TimeUtil.parseLong2String(date.getTime()));
                        ps.setLong(3,date.getTime());
                        ps.setString(4,TimeUtil.parseLong2String(date.getTime()));
                        ps.execute();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }finally {
                    JdbcUtil.close(conn,ps,null);
                }
            }

        }
        //然后给map中进行去重操作，Set
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimention().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //输出
        context.write(key,this.v);
    }
}
