package com.phone.analystic.mr.nm;

import com.phone.Util.JdbcUtil;
import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.OutputMySqlFormat;
import com.phone.analystic.mr.am.ActiveMemberMapper;
import com.phone.analystic.mr.am.ActiveMemberReducer;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.Constants;
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
 * activeUser的runner方法
 */
public class NewMemberRunner implements Tool{
    private static Logger logger = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = new Configuration();


    @Override
    public void setConf(Configuration conf) {
        //将resources中的xml文件资源添加到conf中，以后就可以找到他们
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        conf.addResource("new_total_mapping.xml");
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

        job.setJarByClass(NewMemberRunner.class);


        //设置map相关
        job.setMapperClass(NewMemberMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce相关
        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //设置输入路径
        handleInput(job);
        //设置reduce的输出格式类
        job.setOutputFormatClass(OutputMySqlFormat.class);

        if(job.waitForCompletion(true)) {
            handldTotalMembers(job);
            return 0;
        }else{
            return 1;
        }

    }

    private void handldTotalMembers(Job job) {
        IDimension iDimension = new IDimensionImpl();

        String date = job.getConfiguration().get(GlobalConstants.RUNNING_DATE);
        long nowday = TimeUtil.parseString2Long(date);
        long yesterday = nowday - GlobalConstants.DAY_OF_MILISECONDS;

        DateDimension nowdayDimension = DateDimension.buildDate(nowday, DateEnum.DAY);
        DateDimension yesterdayDimension = DateDimension.buildDate(yesterday,DateEnum.DAY);

        int nowdayId = -1;
        int yesterdayId = -1;

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String,Integer> map = new HashMap<String,Integer>();

        try {
            nowdayId = iDimension.getDimensionIdByObject(nowdayDimension);
            yesterdayId = iDimension.getDimensionIdByObject(yesterdayDimension);

            if(nowdayId > 0){
                conn = JdbcUtil.getConn();
                ps = conn.prepareStatement(conf.get("nowday_new_member"));
                ps.setInt(1,nowdayId);
                rs = ps.executeQuery();
                while(rs.next()){
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int browserDimensionId = rs.getInt("browser_dimension_id");
                    int newMembers = rs.getInt("new_members");
                    map.put(platformDimensionId+"_"+browserDimensionId,newMembers);
                }
            }

            if(yesterdayId > 0){
                conn = JdbcUtil.getConn();
                ps = conn.prepareStatement(conf.get("yesterday_total_members"));
                ps.setInt(1,yesterdayId);
                rs = ps.executeQuery();
                while(rs.next()){
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int browserDimensionId = rs.getInt("browser_dimension_id");
                    int totalMembers = rs.getInt("total_members");
                    if(map.containsKey(platformDimensionId+"_"+browserDimensionId)){
                        totalMembers += map.get(platformDimensionId+"_"+browserDimensionId);
                    }
                    map.put(platformDimensionId+"_"+browserDimensionId,totalMembers);
                }
            }

//            //更新statsNewUser中新增的总用户
//            ps = conn.prepareStatement(conf.get("stats_user_total_members"));
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
            ps = conn.prepareStatement(conf.get("stats_device_browser_total_members"));
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }
    }


    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewMemberRunner(),args);
        } catch (Exception e) {
            logger.warn("执行outputToMysql异常.",e);
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
