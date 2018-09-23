package com.phone.analystic.modle.value;

import com.phone.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @ClassName: StatsOutputValue
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 2:41
 * @Version: 1.0
 * @Description: 封装map或者是reduce阶段的输出value的类型的顶级父类
 */
public abstract class StatsOutputValue implements Writable{
    //获取kpi的抽象方法
    public abstract KpiType getKpi();

}
