package com.phone.analystic.hive;

import com.phone.analystic.modle.base.CurrencyTypeDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: CurrencyTypeDimensionUDF
 * @Author: rtt
 * @Date: 2018/10/4 0004 12:06
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class CurrencyTypeDimensionUDF extends UDF{

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String cut){
        if(StringUtils.isEmpty(cut)){
            cut = GlobalConstants.DEFAULT_VALUE;
        }

        int id = 0;
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(cut);

        try {
            id = iDimension.getDimensionIdByObject(currencyTypeDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return id;
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUDF().evaluate("人民币"));
    }
}
