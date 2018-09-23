package com.phone.analystic.mr.nu;

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
 * @ClassName NewUserMapper
 * @Author rtt
 * @Date $ $
 * @Vesion 1.0
 * @Description //在这里，map的输出类型的key是StatusUserDimention，里面包括DateDimension、PlatformDimension、
 * BrowserDimension、KpiDimension,而输出的类型的value是统计的值，是一个Map类型，比如：(active_users,10)，这种类型
 * 在这里还需解决的是DateDimension中id的生成的问题
 *
 * 用户模块下的新增用户
 **/
public class NewUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    /**
     * 下面这段代码很重要，因为这是区别你给什么字段赋值的依据，其中KpiType.NEW_USER.kpiName，就是等于new_user，所以，
     * 通过这个之后输出到mysql中，才会找到这个字段
     */
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);

    /**
     * 目前只实现了newUserKpi，下面这个是需要实现的，还没有写出来，然后reduce等后面的代码都依赖这
     * 所以以后修改这里面的代码即可，后面的代码都没有具体的那些值，都是从这个类中传入的
     */
    private KpiDimension browserUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);

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
        if(StringUtils.isNotEmpty(en) && en.equals(Constants.EventEnum.LANUCH.alias)){
            //获取想要的字段
            String serverTime = fields[1];
            String platForm = fields[13];
            String uuid =fields[3];

            if(StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(uuid)){
                logger.info("serverTime and uuid is empty");
                return;
            }

            //构造输出的key
            //获取long类型的时间
            long stime = Long.valueOf(serverTime);
            PlatformDimention platformDimention =PlatformDimention.getInstance(platForm);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);

            //K是StatsUserDimension，但因为他继承了StatsCommonDimension，所以k先在子类中查找，找不到
            //就会去父类中查找，所以用父类的getStatsCommonDimention()就得到父类的对象
            StatsCommonDimention statsCommonDimention = this.k.getStatsCommonDimention();

            //为statsCommonDimention设值
            statsCommonDimention.setDateDimension(dateDimension);
            statsCommonDimention.setPlatformDimention(platformDimention);
            statsCommonDimention.setKpiDimension(newUserKpi);

            //设置默认的浏览器对象
            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            this.k.setBrowserDimension(defaultBrowserDimension);

            this.k.setStatsCommonDimention(statsCommonDimention);

            //构建输出的value
            this.v.setId(uuid);
            //输出
            context.write(this.k,this.v);
        }
    }
}