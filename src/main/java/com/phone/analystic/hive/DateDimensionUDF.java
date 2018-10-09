package com.phone.analystic.hive;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: DateDimensionUDF
 * @Author: rtt
 * @Date: 2018/9/27 0027 21:59
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class DateDimensionUDF extends UDF{

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String date){
        if(StringUtils.isEmpty(date)){
            date = TimeUtil.getYesterday();
        }

        int id = -1;
        try {
            DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parseString2Long(date), DateEnum.DAY);
            id = iDimension.getDimensionIdByObject(dateDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new DateDimensionUDF().evaluate("2018-09-25"));
    }
}
