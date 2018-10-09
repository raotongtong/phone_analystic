package com.phone.analystic.hive;

import com.phone.analystic.modle.base.EventDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: EventDimensionUDF
 * @Author: rtt
 * @Date: 2018/9/27 0027 21:38
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class EventDimensionUDF extends UDF{

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }

        int id = -1;
        try {
            EventDimension eventDimension = new EventDimension(category,action);
            id = iDimension.getDimensionIdByObject(eventDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

}
