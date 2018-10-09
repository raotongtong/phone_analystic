package com.phone.analystic.mr.nu;

import com.phone.Util.JdbcUtil;
import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.OutputMySqlFormat;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.DateEnum;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: NewUserRunner
 * @Author: rtt
 * @Date: 2018/9/21 0021 下午 3:18
 * @Version: 1.0
 * @Description: java类作用描述
 *
 *  truncate dimension_browser;
    truncate dimension_currency_type;
    truncate dimension_date;
    truncate dimension_event;
    truncate dimension_inbound;
    truncate dimension_kpi;
    truncate dimension_location;
    truncate dimension_os;
    truncate dimension_payment_type;
    truncate dimension_platform;
    truncate event_info;
    truncate order_info;
    truncate stats_device_browser;
    truncate stats_device_location;
    truncate stats_event;
    truncate stats_hourly;
    truncate stats_inbound;
    truncate stats_order;
    truncate stats_user;
    truncate stats_view_depth;
 */
public class NewUserRunner implements Tool{
    private static Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();


    @Override
    public void setConf(Configuration conf) {
        //将resources中的xml文件资源添加到conf中，以后就可以找到他们
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        conf.addResource("new_total_mapping.xml");
//        conf.set("mapred.jar","phone_analystic-1.0-SNAPSHOT.jar");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //处理参数,一定要设置在job之前，才能生效
        handleArgs(conf,strings);

        Job job = Job.getInstance(conf);

        job.setJarByClass(NewUserRunner.class);


        //设置map相关
        job.setMapperClass(NewUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce相关
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //设置输入路径
        handleInput(job);
        //设置reduce的输出格式类
        job.setOutputFormatClass(OutputMySqlFormat.class);

//        job.setNumReduceTasks(4);
//        return job.waitForCompletion(true)?0:1;

        if(job.waitForCompletion(true)){
            this.computeTotalNewUser(job);
            return 0;
        }else{
            return 1;
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewUserRunner(),args);
        } catch (Exception e) {
            logger.warn("执行outputToMysql异常.",e);
        }
    }

    /**
     * 计算新增的总用户
     * 1、获取运行日期当天和前一天的时间维度，并获取其对应的时间维度id，判断id是否大于0。
     * 2、根据时间维度的id获取前天的总用户和当天的新增用户。
     * 3、更新新增总用户
     * @param
     */

    /**
     * 总结：为什么写不出来？
     * 1、通过指定的时间找昨天还有明天，用long类型，然后来加减一天的long类型的数值，86400000L;//24*60*60*1000代表一天
     * 2、一般的时间类型的转换，要熟悉
     * 3、要熟悉jdbc操作数据库的代码过程
     * 4、在项目中，一定要合理来使用map来存储数据
     *
     */
    private void computeTotalNewUser(Job job) {
        IDimension iDimension = new IDimensionImpl();
        Connection conn = null;
        PreparedStatement ps =null;
        ResultSet rs = null;

        String nowday = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        long nowdayWithLong = TimeUtil.parseString2Long(nowday);
        long yesterdayWithLong = nowdayWithLong - GlobalConstants.DAY_OF_MILISECONDS;

        DateDimension nowdayDateDimension = DateDimension.buildDate(nowdayWithLong, DateEnum.DAY);
        DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterdayWithLong, DateEnum.DAY);

        int nowdayId = -1;
        int yesterdayId = -1;

        try {
            conn = JdbcUtil.getConn();
            Map<String,Integer> map = new HashMap<String,Integer>();

            //这里生成了date_dimension表中RUNNING_DATE的时间
            nowdayId = iDimension.getDimensionIdByObject(nowdayDateDimension);
            yesterdayId = iDimension.getDimensionIdByObject(yesterdayDateDimension);

            if(yesterdayId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.YESTERDAY_TOTAL_USER));
                ps.setInt(1, yesterdayId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int browserDimensionId = rs.getInt("browser_dimension_id");
                    int yesterdayTotalNewUser = rs.getInt("total_install_users");
                    //存储
                    map.put(platformDimensionId+"_"+browserDimensionId,yesterdayTotalNewUser);
                }
            }

            if(nowdayId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.NOWDAY_NEW_USER));
                ps.setInt(1, nowdayId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int browserDimensionId= rs.getInt("browser_dimension_id");
                    int nowdayNewUser = rs.getInt("new_install_users");
                    //存储
                    if(map.containsKey(platformDimensionId+"_"+browserDimensionId)){
                        nowdayNewUser += map.get(platformDimensionId+"_"+browserDimensionId);
                    }
                    map.put(platformDimensionId+"_"+browserDimensionId,nowdayNewUser);
                }
            }

            //更新statsNewUser中新增的总用户
            //这里有点问题，因为statsNewUser表不考虑浏览器维度，所以这个表的数据有点不对
//            ps = conn.prepareStatement(conf.get(GlobalConstants.NOWDAY_NEW_TOTAL_USER));
//            for(Map.Entry<String,Integer> en : map.entrySet()){
//                String[] split = en.getKey().split("_");
//                ps.setInt(1,nowdayId);
//                ps.setInt(2,Integer.parseInt(split[0]));
//                ps.setInt(3,en.getValue());
//                ps.setString(4,conf.get(GlobalConstants.RUNNING_DATE));
//                ps.setInt(5,en.getValue());
//                ps.execute();
//            }

            //更新statsBrowserNewUser中的新增总用户
            ps = conn.prepareStatement(conf.get(GlobalConstants.STATS_DEVICE_BROWSER_TOTAL_NEW_USERS));
            for(Map.Entry<String,Integer> en : map.entrySet()){
                String[] split = en.getKey().split("_");
                ps.setInt(1,nowdayId);
                ps.setInt(2,Integer.parseInt(split[0]));
                ps.setInt(3,Integer.parseInt(split[1]));
                ps.setInt(4,en.getValue());
                ps.setString(5,conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(6,en.getValue());
                ps.execute();
            }
        } catch (Exception e) {
            logger.error("统计总新增用户失败！",e);
        } finally {
            JdbcUtil.close(conn,ps,rs);
        }

    }


    //处理参数
    private void handleArgs(Configuration conf, String[] args) {
        String date = null;
        if(args.length > 0){
            //循环args
            for(int i = 0 ; i<args.length;i++){
                //判断参数中是否有-d
                if(args[i].equals("-d")){
                    if(i+1 <= args.length){
                        date = args[i+1];
                        break;
                    }
                }
            }

            //判断
            if(StringUtils.isEmpty(date)){
                date = TimeUtil.getYesterday();
            }
            //将date存储到conf中
            conf.set(GlobalConstants.RUNNING_DATE,date);
        }
    }

    /**
     * 设置输入输出
     * @param job
     */
    private void handleInput(Job job) {
        String [] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inpath = new Path("/ods/"+month+"/"+day);
            if(fs.exists(inpath)){
                FileInputFormat.addInputPath(job,inpath);
            } else {
                throw  new RuntimeException("输入路径不存储在.inpath:"+inpath.toString());
            }
        } catch (IOException e) {
            logger.warn("设置输入路径异常.",e);
        }
    }



}
