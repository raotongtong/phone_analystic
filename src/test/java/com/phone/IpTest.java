package com.phone;

import com.phone.etl.ip.IPSeeker;
import com.phone.etl.ip.LogUtil;
import com.phone.etl.ip.UserAgentUtil;

import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class IpTest {
    public static void main(String[] args) {
//        System.out.println(IPSeeker.getInstance().getCountry("112.111.11.12"));

//        System.out.println(IpUtil.getRegionInfoByIp("112.111.11.12"));
//
//        try {
//            System.out.println(IpUtil.parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip=112.111.11.12","utf-8"));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

//        UserAgentUtil.UserAgentInfo userAgentInfo = UserAgentUtil.parserUserAgent("Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36");
//
//        String str = "3^A1531110990.123^Ahh^A/BC";
//        String[] split = str.split("\\^A");
//        for(String s:split){
//            System.out.println(s);
//        }



        Map<String, String> map = LogUtil.parserLog("114.61.94.253^A1531110990.123" +
                "^Ahh^A/BCImg.gif?en=e_l&ver=1&pl=website&sdk=js&u_ud=27F69684-BBE3-42" +
                "FA-AA62-71F98E208444&u_mid=Aidon&u_sd=38F66FBB-C6D6-4C1C-8E05-72C31675C00" +
                "A&c_time=1449917532123&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1" +
                "%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome" +
                "%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768");
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + entry.getValue());
        }
    }
}