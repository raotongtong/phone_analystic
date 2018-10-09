package com.phone.analystic.hive;

import com.phone.analystic.modle.base.PlatformDimention;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: PlatFormDimensionUDF
 * @Author: rtt
 * @Date: 2018/9/27 0027 23:06
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class PlatFormDimensionUDF extends UDF{

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String platForm){
        if(StringUtils.isEmpty(platForm)){
            platForm = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;

        PlatformDimention platformDimention = new PlatformDimention(platForm);

        try {
            id = iDimension.getDimensionIdByObject(platformDimention);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return id;

    }

    public static void main(String[] args) {
        System.out.println(new PlatFormDimensionUDF().evaluate("website"));
    }
}
