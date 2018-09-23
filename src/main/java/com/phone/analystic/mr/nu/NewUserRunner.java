package com.phone.analystic.mr.nu;

import com.phone.analystic.mr.OutputMySqlFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @ClassName: NewUserRunner
 * @Author: rtt
 * @Date: 2018/9/21 0021 下午 3:18
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class NewUserRunner implements Tool{
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
        return null;
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置reduce的输出格式类
        job.setOutputFormatClass(OutputMySqlFormat.class);

        return 0;
    }




}
