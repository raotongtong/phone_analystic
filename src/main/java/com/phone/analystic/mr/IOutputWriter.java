package com.phone.analystic.mr;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.analystic.mr.service.IDimension;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * @ClassName: IOutputWriter
 * @Author: rtt
 * @Date: 2018/9/21 0021 上午 11:55
 * @Version: 1.0
 * @Description: 操作结果表的接口
 */
public interface IOutputWriter {

    /**
     * 为每一个kpi的最终结果赋值的接口，最终的赋值的类在各个kpi的包中
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param iDimension
     */
    void output(Configuration conf,
                StatsBaseDimension key,
                StatsOutputValue value,
                PreparedStatement ps,
                IDimension iDimension);


}
