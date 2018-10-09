package com.phone.analystic.mr.location;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.analystic.modle.value.reduce.LocationOutputWritable;
import com.phone.analystic.mr.IOutputWriter;
import com.phone.analystic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName: LocationOutputWritter
 * @Author: rtt
 * @Date: 2018/9/28 0028 13:05
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class LocationOutputWritter implements IOutputWriter{

    @Override
    public void output(Configuration conf, StatsBaseDimension key,
                       StatsOutputValue value, PreparedStatement ps,
                       IDimension iDimension) {

        StatsLocationDimension k = (StatsLocationDimension)key;

        int aus = ((LocationOutputWritable) value).getAus();
        int sessions = ((LocationOutputWritable) value).getSessions();
        int bounceSession = ((LocationOutputWritable) value).getBounceSession();

        try {
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimention().getPlatformDimention()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getLocationDimension()));
            ps.setInt(++i,aus);
            ps.setInt(++i,sessions);
            ps.setInt(++i,bounceSession);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,aus);
            ps.setInt(++i,sessions);
            ps.setInt(++i,bounceSession);

            ps.addBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
