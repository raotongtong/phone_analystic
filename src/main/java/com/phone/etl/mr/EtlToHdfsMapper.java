package com.phone.etl.mr;

import com.phone.common.Constants;
import com.phone.etl.ip.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;


/**
 * @ClassName EtlToHdfsMapper
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description
 **/
public class EtlToHdfsMapper extends Mapper<LongWritable,Text,LogWritable,NullWritable> {
    private static final Logger logger = Logger.getLogger(EtlToHdfsMapper.class);
    private LogWritable k = new LogWritable();
    private int inputRecords,filterRecords,outputRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            this.inputRecords ++;
            //空行处理
            if(StringUtils.isEmpty(line)){
                this.filterRecords ++;
                return ;
            }

            //调用logutil中的parseLog()方法，返回map，然后循环map将数据分别输出
            //可以根据事件来分别输出？？？
            Map<String,String> map = LogUtil.parserLog(line);


            //获取事件的名
            String eventName = map.get(Constants.LOG_EVENT_NAME);
            Constants.EventEnum event = Constants.valueOfAlias(eventName);

            switch (event){
                case LANUCH:
                case EVENT:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGESUCCESS:
                case CHARGEREFUND:
                    handleLog(map,context); //处理输出
                    break;

                default:
                    break;
            }
        } catch (Exception e) {
            this.filterRecords ++;
            logger.warn("处理mapper写出数据的时候异常.",e);
        }
    }

    /**
     * 处理真正的将map中的所有k-v进行输出
     * @param map
     * @param context
     */
    private void handleLog(Map<String,String> map, Context context) {

        try {
            //map循环
            for(Map.Entry<String,String> en : map.entrySet()){
                this.k.setB_iev(en.getValue());
                switch (en.getKey()){
                    case "ver": this.k.setIp(en.getValue()); break;
                    case "s_time": this.k.setS_time(en.getValue()); break;
                    case "en": this.k.setEn(en.getValue()); break;
                    case "u_ud": this.k.setU_ud(en.getValue()); break;
                    case "u_mid": this.k.setU_mid(en.getValue()); break;
                    case "u_sd": this.k.setU_sd(en.getValue()); break;
                    case "c_time": this.k.setC_time(en.getValue()); break;
                    case "l": this.k.setL(en.getValue()); break;
                    case "b_iev": this.k.setB_iev(en.getValue()); break;
                    case "b_rst": this.k.setB_rst(en.getValue()); break;
                    case "p_url": this.k.setP_url(en.getValue()); break;
                    case "p_ref": this.k.setP_ref(en.getValue()); break;
                    case "tt": this.k.setTt(en.getValue()); break;
                    case "pl": this.k.setPl(en.getValue()); break;
                    case "ip": this.k.setIp(en.getValue()); break;
                    case "oid": this.k.setOid(en.getValue()); break;
                    case "on": this.k.setOn(en.getValue()); break;
                    case "cua": this.k.setCua(en.getValue()); break;
                    case "cut": this.k.setCut(en.getValue()); break;
                    case "pt": this.k.setPt(en.getValue()); break;
                    case "ca": this.k.setCa(en.getValue()); break;
                    case "ac": this.k.setAc(en.getValue()); break;
                    case "kv_": this.k.setKv_(en.getValue()); break;
                    case "du": this.k.setDu(en.getValue()); break;
                    case "browserName": this.k.setBrowserName(en.getValue()); break;
                    case "browserVersion": this.k.setBrowserVersion(en.getValue()); break;
                    case "osName": this.k.setOsName(en.getValue()); break;
                    case "osVersion": this.k.setOsVersion(en.getValue()); break;
                    case "country": this.k.setCountry(en.getValue()); break;
                    case "province": this.k.setProvince(en.getValue()); break;
                    case "city": this.k.setCity(en.getValue()); break;
                }
            }
            this.outputRecords ++;
            context.write(k,NullWritable.get());
        } catch (Exception e) {
            logger.warn("etl最终输出异常",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("inputRecords:"+this.inputRecords+"  filterRecords:"+filterRecords+"  outputRecords:"+outputRecords);
    }
}