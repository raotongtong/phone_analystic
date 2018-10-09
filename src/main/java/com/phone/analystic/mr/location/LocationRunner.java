package com.phone.analystic.mr.location;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.value.map.LocationOutputValue;
import com.phone.analystic.modle.value.reduce.LocationOutputWritable;
import com.phone.analystic.mr.OutputMySqlFormat;
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

/**
 * @ClassName: LocationRunner
 * @Author: rtt
 * @Date: 2018/9/28 0028 12:51
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class LocationRunner implements Tool{
    private static Logger logger = Logger.getLogger(LocationRunner.class);
    Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        handleArgs(conf,args);

        Job job = Job.getInstance(conf,"location runner");

        job.setJarByClass(LocationRunner.class);

        job.setMapperClass(LocationMapper.class);
        job.setMapOutputKeyClass(StatsLocationDimension.class);
        job.setMapOutputValueClass(LocationOutputValue.class);

        job.setReducerClass(LocationReducer.class);
        job.setOutputKeyClass(StatsLocationDimension.class);
        job.setOutputValueClass(LocationOutputWritable.class);

        handleInput(job);

        job.setOutputFormatClass(OutputMySqlFormat.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new LocationRunner(),args);
        } catch (Exception e) {
            logger.error("执行ouput to mysql失败",e);
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
