package com.phone.etl.mr;

import com.phone.Util.TimeUtil;
import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName EtlToHdfsRunner
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 驱动类
 *  * 原数据：/log/09/18
 *  * 原数据：/log/09/19
 *  * 清洗后的存储目录: /ods/09/18
 *  * 清洗后的存储目录: /ods/09/19
 * yarn jar ./   package.classname -d 2018-09-19
 **/
public class EtlToHdfsRunner implements Tool {
    private static final Logger logger = Logger.getLogger(EtlToHdfsRunner.class);
    private Configuration conf = new Configuration();

    //主函数
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new EtlToHdfsRunner(),args);
        } catch (Exception e) {
            logger.warn("执行etl to hdfs异常.",e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        conf = this.conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //1、获取-d之后的日期并存储到conf中，如果没有-d或者日期不合法则使用昨天为默认值
        this.handleArgs(conf,args);
        //获取job
        Job job= Job.getInstance(conf,"etl to hdfs");

        job.setJarByClass(EtlToHdfsRunner.class);

        //设置map相关
        job.setMapperClass(EtlToHdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //没有reduce
        job.setNumReduceTasks(0);

        //设置输入输出
        this.handleInputOutpu(job);
        return job.waitForCompletion(true)?1:0;
    }


    /**
     *
     * @param configuration
     * @param args
     */
    private void handleArgs(Configuration configuration, String[] args) {
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
    private void handleInputOutpu(Job job) {
        String [] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            Path inpath = new Path("/log/"+month+"/"+day);
            Path outpath = new Path("/ods/"+month+"/"+day);
            if(fs.exists(inpath)){
                FileInputFormat.addInputPath(job,inpath);
            } else {
                throw  new RuntimeException("输入路径不存储在.inpath:"+inpath.toString());
            }
            //设置输出
            if(fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            //设置输出
            FileOutputFormat.setOutputPath(job,outpath);
        } catch (IOException e) {
            logger.warn("设置输入输出路径异常.",e);
        }
    }
}