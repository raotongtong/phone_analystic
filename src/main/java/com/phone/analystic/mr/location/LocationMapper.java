package com.phone.analystic.mr.location;

import com.phone.analystic.modle.StatsCommonDimention;
import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.*;
import com.phone.analystic.modle.value.map.LocationOutputValue;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.common.Constants;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 地域模块下的统计：
 * 活跃用户：所有维度下uuid的去重个数
 * 会话个数：所有维度下u_sd的去重个数
 * 跳出会话个数：u_sd只出现一次的总个数
 **/
public class LocationMapper extends Mapper<LongWritable,Text,StatsLocationDimension,LocationOutputValue> {
    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private StatsLocationDimension k = new StatsLocationDimension();
    private LocationOutputValue v = new LocationOutputValue();

    private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        //拆分
        String[] fields = line.split("\u0001");
        /**
         * 这里得到时间的指标，等到以后有其他指标，就从这里进行修改
         */
        //获取想要的字段
        String serverTime = fields[1];
        String platForm = fields[13];
        String uuid = fields[3];
        String sessionId = fields[5];
        String country = fields[28];
        String province = fields[29];
        String city = fields[30];


        if (StringUtils.isEmpty(serverTime) ||
                StringUtils.isEmpty(platForm) ||
                StringUtils.isEmpty(uuid) ||
                StringUtils.isEmpty(sessionId) ||
                StringUtils.isEmpty(country) ||
                StringUtils.isEmpty(province) ||
                StringUtils.isEmpty(city)) {
            logger.info("serverTime or uuid or platform or country or province or city is empty");
            return;
        }

        //构造输出的key
        //获取long类型的时间
        long stime = Long.valueOf(serverTime);
        PlatformDimention platformDimention = PlatformDimention.getInstance(platForm);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);

        StatsCommonDimention statsCommonDimention = this.k.getStatsCommonDimention();

        //为statsCommonDimention设值
        statsCommonDimention.setDateDimension(dateDimension);
        statsCommonDimention.setPlatformDimention(platformDimention);
        statsCommonDimention.setKpiDimension(locationKpi);
        LocationDimension locationDimension = new LocationDimension(country,province,city);

        this.k.setLocationDimension(locationDimension);
        this.k.setStatsCommonDimention(statsCommonDimention);
        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);

        context.write(this.k, this.v);


    }
}