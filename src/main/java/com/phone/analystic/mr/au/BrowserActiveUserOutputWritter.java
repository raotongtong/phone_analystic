package com.phone.analystic.mr.au;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.IOutputWriter;
import com.phone.analystic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;

/**
 * @ClassName: BrowserNewUserOutputWritter
 * @Author: rtt
 * @Date: 2018/9/25 0025 上午 11:43
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class BrowserActiveUserOutputWritter implements IOutputWriter {
    @Override
    public void output(Configuration conf, StatsBaseDimension key,
                       StatsOutputValue value, PreparedStatement ps,
                       IDimension iDimension) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            int i = 0;

            switch (v.getKpi()){
                case ACTIVE_USER:
                case BROWSER_ACTIVE_USER:
                    //获取新增用户的值
                    int activeUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();

                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getDateDimension()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getPlatformDimention()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                    ps.setInt(++i,activeUser);
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(++i,activeUser);
                    break;

                case HOURLY_ACTIVE_USER:
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getDateDimension()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getPlatformDimention()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(new KpiDimension(v.getKpi().kpiName)));

                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(v.getValue().get(new IntWritable(j)))).get());
                    }
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(v.getValue().get(new IntWritable(j)))).get());
                    }
                    break;
            }

            ps.addBatch(); //添加到批处理中，在OutputMySqlFormat中，有ps.executeBatch(),就可以执行这个批次


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
