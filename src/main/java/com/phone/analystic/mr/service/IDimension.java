package com.phone.analystic.mr.service;

import com.phone.analystic.modle.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: IDimension
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 9:26
 * @Version: 1.0
 * @Description: 根据维度获取对应的id的接口
 *
 * dimension:基础维度对象
 *
 * 接口省略了public
 */
public interface IDimension {
    int getDimensionIdByObject(BaseDimension dimension) throws IOException,SQLException;
}
