package com.phone.analystic.mr.am;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.IOutputWriter;
import com.phone.analystic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;

/**
 * @ClassName: NewUserOutputWritter
 * @Author: rtt
 * @Date: 2018/9/21 0021 下午 2:36
 * @Version: 1.0
 * @Description: 在resources文件夹中output_writter.xml这个文件中有这个包名 + 类名，就是给sql语句赋值用的
 */
public class ActiveMemberOutputWritter implements IOutputWriter{
    @Override
    public void output(Configuration conf, StatsBaseDimension key,
                       StatsOutputValue value, PreparedStatement ps,
                       IDimension iDimension) {

        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;

            //获取新增用户的值
            int activeMember = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();

            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getPlatformDimention()));
            ps.setInt(++i,activeMember);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,activeMember);

            ps.addBatch(); //添加到批处理中，在OutputMySqlFormat中，有ps.executeBatch(),就可以执行这个批次

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
