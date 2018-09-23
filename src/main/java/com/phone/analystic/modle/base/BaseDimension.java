package com.phone.analystic.modle.base;

import org.apache.hadoop.io.WritableComparable;

/**
 * @ClassName BaseDimension
 * @Author rtt
 * @Date $ $
 * @Vesion 1.0
 * @Description 所有基础维度类的顶级父类
 *
 *  SELECT
    SUM(sdb.new_install_users)
    FROM stats_device_browser sdb
    LEFT JOIN dimension_date dd
    ON dd.id = sdb.date_dimension_id
    LEFT JOIN dimension_platform dp
    ON sdb.platform_dimension_id = dp.id
    LEFT JOIN dimension_browser db
    ON sdb.browser_dimension_id = db.id
    WHERE dd.calendar = "2018-09-20"
    and dp.platform_name = "ios"
    and db.browser_name = "IE"
    #GROUP BY sdb.date_dimension_id,sdb.platform_dimension_id,sdb.browser_dimension
;
 *
 **/
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
}