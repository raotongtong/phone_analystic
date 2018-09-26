package com.phone.analystic.mr.am;

import com.phone.analystic.modle.StatsCommonDimention;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.BrowserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimention;
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
 * 用户模块下的活跃用户
 **/
public class ActiveMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.kpiName);
    private KpiDimension browserActiveMemberKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.kpiName);

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
        String en = fields[2];
        if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.PAGEVIEW.alias)) {
            //获取想要的字段
            String serverTime = fields[1];
            String platForm = fields[13];
            String memberid = fields[4];
            String browserName = fields[24];
            String browserVersion = fields[25];

            if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(memberid)) {
                logger.info("serverTime and uuid is empty");
                return;
            }

            //构造输出的key
            //获取long类型的时间
            long stime = Long.valueOf(serverTime);
            PlatformDimention platformDimention = PlatformDimention.getInstance(platForm);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            BrowserDimension browserDimension = BrowserDimension.getInstance(browserName, browserVersion);

            //K是StatsUserDimension，但因为他继承了StatsCommonDimension，所以k先在子类中查找，找不到
            //就会去父类中查找，所以用父类的getStatsCommonDimention()就得到父类的对象
            StatsCommonDimention statsCommonDimention = this.k.getStatsCommonDimention();

            //为statsCommonDimention设值
            statsCommonDimention.setDateDimension(dateDimension);
            statsCommonDimention.setPlatformDimention(platformDimention);
            statsCommonDimention.setKpiDimension(activeMemberKpi);


            //设置默认的浏览器对象
            BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDimention(statsCommonDimention);

            //构建输出的value
            this.v.setId(memberid);

            context.write(this.k, this.v);

            //以下输出的数据用于计算浏览器模块下的新增用户
            statsCommonDimention.setKpiDimension(browserActiveMemberKpi);
            this.k.setBrowserDimension(browserDimension);
            this.k.setStatsCommonDimention(statsCommonDimention);
            //输出
            context.write(this.k, this.v);
        }

    }
}