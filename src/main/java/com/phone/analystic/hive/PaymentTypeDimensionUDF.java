package com.phone.analystic.hive;

import com.phone.analystic.modle.base.PaymentTypeDimension;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @ClassName: PaymentTypeDimensionUDF
 * @Author: rtt
 * @Date: 2018/10/4 0004 12:46
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class PaymentTypeDimensionUDF extends UDF{

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String paymentType){
        if(StringUtils.isEmpty(paymentType)){
            paymentType = GlobalConstants.DEFAULT_VALUE;
        }

        int id = -1;
        PaymentTypeDimension paymentTypeDimension = new PaymentTypeDimension(paymentType);

        try {
            id = iDimension.getDimensionIdByObject(paymentTypeDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PaymentTypeDimensionUDF().evaluate(
                "支付宝"
        ));
    }

}
