package com.phone.etl.ip;


import com.phone.common.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 处理正行的日志
     * @param log :221.13.21.192^A1535611950.612^A221.13.21.192^A/
     *            shopping.jsp?c_time=1535611600666&oid=123461&u_mid=dc64823d-5cb7-4e3d-8a87-fa2b4e096ea0&pl=java_server&en=e_cs&sdk=jdk&ver=1
     * @return
     */
    public static Map<String,String> parserLog(String log){
        //ConcurrentHashMap是线程安全的
        Map<String, String> map = new ConcurrentHashMap<>();
        if(StringUtils.isNotEmpty(log)){
            String[] fields = log.split("\\^A");
            //为了判断一个日志文件是否完整，这里先判断一下长度
            if(fields.length == 4){

                //将ip和时间戳的字段名和值存储map中
                //注：在这里也可以直接对ip和时间戳进行解析,在这里为了整体性
                map.put(Constants.LOG_IP,fields[0]);
                map.put(Constants.LOG_SERVER_TIME,fields[1].replaceAll("\\.",""));
                //剩下的一些参数列表，单独处理
                String params = fields[3];
                handleParams(params,map);
                //处理ip解析
                handleIp(map);
                //处理UserAgent戳解析
                handleUserAgent(map);
            }
        }
        return map;
    }

    /**
     * 将map的关于useragent的信息取出，并进行处理
     * @param map
     */
    private static void handleUserAgent(Map<String, String> map) {
        if(map.containsKey(Constants.LOG_USERAGENT)){
            String userAgent = map.get(Constants.LOG_USERAGENT);
            UserAgentUtil.UserAgentInfo userAgentInfo = UserAgentUtil.parserUserAgent(userAgent);
            //将值存储到map中
            map.put(Constants.LOG_BROWSER_NAME,userAgentInfo.getBrowser());
            map.put(Constants.LOG_BROWSER_VERSION,userAgentInfo.getBrowserVersion());
            map.put(Constants.LOG_OS_NAME,userAgentInfo.getSystem());
            map.put(Constants.LOG_OS_VERSION,userAgentInfo.getSystemVersion());
        }
    }


    /**
     * 将map中的ip取出来，然后解析成国家省市，最后只能够存储到map中
     * @param map
     */
    private static void handleIp(Map<String, String> map) {
        if(map.containsKey(Constants.LOG_IP)){
            IpUtil.RegionInfo info = IpUtil.getRegionInfoByIp(map.get(Constants.LOG_IP));
            //将值存储到map中
            map.put(Constants.LOG_COUNTRY,info.getCountry());
            map.put(Constants.LOG_PROVINCE,info.getProvince());
            map.put(Constants.LOG_CITY,info.getProvince());
        }
    }

    /**
     *
     * @param params
     * @param map
     * shopping.jsp?c_time=1535611600666&oid=123461&u_mid=dc64823d-5cb7-4e3d-8a87-fa2b4e096ea0&pl=java_server&en=e_cs&sdk=jdk&ver=1
     */
    private static void handleParams(String params, Map<String, String> map) {
        try {
            if(StringUtils.isNotEmpty(params)){
                int index = params.indexOf("?");
                if(index > 0){
                    String[] fields = params.substring(index + 1).split("&");
                    for(String field : fields){
                        String[] kvs = field.split("=");
                        //这种方式可以将URL解码，但更好的方式是调用URLDecoder
                        //String str = userAgent.replace("%2F", "/").replace("%20", " ").replace("%3B", ";")
                        //        .replace("%2C", ",");
                        //System.out.println(str);
                        String v = URLDecoder.decode(kvs[1],"utf-8");
                        String k = kvs[0];
                        //判断key是否为空
                        if(StringUtils.isNotEmpty(k)){
                            //存储到map中
                            map.put(k,v);
                        }
                    }
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.warn("value进行urldecode解码异常",e);
        }
    }
}