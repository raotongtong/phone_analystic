package com.phone.analystic.mr.b_pv;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.OutputMySqlFormat;
import com.phone.analystic.mr.am.ActiveMemberMapper;
import com.phone.analystic.mr.am.ActiveMemberReducer;
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
 * activeUser的runner方法
 */
public class PVRunner implements Tool{
    private static Logger logger = Logger.getLogger(PVRunner.class);
    private Configuration conf = new Configuration();


    @Override
    public void setConf(Configuration conf) {
        //将resources中的xml文件资源添加到conf中，以后就可以找到他们
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
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

        job.setJarByClass(PVRunner.class);


        //设置map相关
        job.setMapperClass(PVMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        //设置reduce相关
        job.setReducerClass(PVReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(OutputWritable.class);

        //设置输入路径
        handleInput(job);
        //设置reduce的输出格式类
        job.setOutputFormatClass(OutputMySqlFormat.class);

        return job.waitForCompletion(true)?0:1;

    }


    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new PVRunner(),args);
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
