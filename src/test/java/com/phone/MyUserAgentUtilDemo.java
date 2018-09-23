package com.phone;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;

public class MyUserAgentUtilDemo {
    static UASparser uasParser = null;

    // 初始化uasParser对象
    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
//        String str = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36";
        String s = "Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36";
        String str = s.replace("%2F", "/").replace("%20", " ").replace("%3B", ";")
                .replace("%2C", ",");
        System.out.println(str);
        try {
            UserAgentInfo userAgentInfo = MyUserAgentUtilDemo.uasParser.parse(str);
            System.out.println("操作系统名称："+userAgentInfo.getOsFamily());//
            System.out.println("操作系统："+userAgentInfo.getOsName());//
            System.out.println("浏览器名称："+userAgentInfo.getUaFamily());//
            System.out.println("浏览器版本："+userAgentInfo.getBrowserVersionInfo());//
            System.out.println("设备类型："+userAgentInfo.getDeviceType());
            System.out.println("浏览器:"+userAgentInfo.getUaName());
            System.out.println("类型："+userAgentInfo.getType());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
