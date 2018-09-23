package com.phone.analystic.mr;

import com.phone.Util.JdbcUtil;
import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.service.IDimension;
import com.phone.analystic.mr.service.impl.IDimensionImpl;
import com.phone.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: OutputMySqlFormat
 * @Author: rtt
 * @Date: 2018/9/21 0021 下午 12:01
 * @Version: 1.0
 * @Description: 将结果输出到mysql的自定义
 */

/**
 * OutputFormat<StatsBaseDimension,OutputWritable>，泛型中要与reduce输出的相一致，因为这个类就是为了将reduce输出的结果写到
 * mysql中，相当于之间我们写的FileOutputFormat(job,path),所以，reduce的输出类OutputFormat，后面的泛型就要和reduce的那个类的
 * 泛型一致，这样才能传递输出的结果
 */
public class OutputMySqlFormat extends OutputFormat<StatsBaseDimension,OutputWritable>{
    //DBOutputFormat

    /**
     * 获取输出记录
     * 通过继承OutputFormat<StatsBaseDimension,OutputWritable>，才能通过下面的方法来得到reduce输出的数据
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<StatsBaseDimension, OutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
        IDimension iDimension = new IDimensionImpl() ;
        return new OutputToMysqlRecordWriter(conf,conn,iDimension);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //检测输出空间
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null,taskAttemptContext);
    }

    /**
     * 用于封装写出记录到mysql的信息
     * StatsBaseDimension，可以得到公共的维度的数据
     * OutputWritable，可以得到kpi(如：new_user)的数据
     */
    public static class OutputToMysqlRecordWriter extends RecordWriter<StatsBaseDimension,OutputWritable>{
        Configuration conf = null;
        Connection conn = null;
        IDimension iDimension = null;
        //存储kpi-ps
        private Map<KpiType,PreparedStatement> map = new HashMap<KpiType,PreparedStatement>();
        //存储kpi-对应的输出的sql，达到多少sql才执行一次
        private Map<KpiType,Integer> batch = new HashMap<KpiType,Integer>();

        public OutputToMysqlRecordWriter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }

        /**
         * 写
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(StatsBaseDimension key, OutputWritable value) throws IOException, InterruptedException {
            //获取kpi(从value中获取)，这里value里面装有具体的kpi的名字，比方说new_user
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            try {
                //获取ps
                if(map.containsKey(kpi)){
                    ps = map.get(kpi);
                }else{
                    //这里就是通过sql语句得到ps，在这里并没有执行，只是将kpi和ps存储到map中，等到50个sql之后，在执行一次，
                    //方法在下面
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,ps);    //将新增加的ps存储到map中
                }
                int count = 1;
                this.batch.put(kpi,count);
                count++;

                //为ps赋值准备
                //得到com.phone.analystic.mr.nu.NewUserOutputWritter这个包名+类名
                String className = conf.get("writter_" + conf.get(kpi.kpiName));
                //通过包名+类名通过反射来给sql语句赋值
                Class<?> classz = Class.forName(className); //将包名 + 类名转换成类
                //这里实例化就可以得到NewUserOutputWritter对象
                IOutputWriter writter = (IOutputWriter)classz.newInstance();
                //调用IOutputWriter中的output方法，就可以给sql语句赋值
                writter.output(conf,key,value,ps,iDimension);

                /**
                 * 老师为啥写的这么麻烦，因为添加批次的操作，是优化的一部分，所以就需要这样写
                 */
                //对赋值好的ps进行执行
                if(batch.size() %50 == 0){ //有50个ps执行
                    ps.executeBatch();  //批量执行，在NewUserOutputWritter.class中执行了ps.batch()，先加入到批次中，这里就是执行
                    conn.commit(); //提交批处理执行
                    batch.remove(kpi);  //将执行完的ps移除掉
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                for(Map.Entry<KpiType,PreparedStatement> en : map.entrySet()){
                    en.getValue().executeBatch();  //将剩余的ps进行执行
                    this.conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                for(Map.Entry<KpiType,PreparedStatement> en : map.entrySet()){
                    JdbcUtil.close(conn,en.getValue(),null); //关闭所有的资源
                }
            }
        }
    }


}
