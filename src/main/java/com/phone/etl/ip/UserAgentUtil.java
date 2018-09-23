package com.phone.etl.ip;

import com.google.common.base.Strings;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;



/**
 * @ClassName UserAgentUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description
 *
 * window.navigator.userAgent
 *
 * Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20
 * (KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36
 * &b_rst=1280*768
 *
 * 修改成：第一个字段：浏览器(chrome) 版本号 系统 版本  第二个字段：分辨率 (1280*768)
 *                  chrome 32.0 windows window 7
 **/
public class UserAgentUtil {
    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);

    static UASparser uasParser = null;


    // 初始化对象
    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("获取uasparser异常.",e);
        }
    }

    /**
     * 解析userAgent
     * @param userAgent
     * @return
     */
    public static UserAgentInfo parserUserAgent(String userAgent){
        UserAgentInfo userAgentInfo = null;

        try {
            if(StringUtils.isNotEmpty(userAgent)){
                //使用uasparser来解析
                cz.mallat.uasparser.UserAgentInfo czUserAgentInfo = UserAgentUtil.uasParser.parse(userAgent);
                if(czUserAgentInfo != null){
                    userAgentInfo = new UserAgentInfo();
                    //操作系统名称
                    userAgentInfo.setSystem(czUserAgentInfo.getOsFamily());
                    //操作系统
                    userAgentInfo.setSystemVersion(czUserAgentInfo.getOsName());
                    //浏览器名称
                    userAgentInfo.setBrowser(czUserAgentInfo.getUaFamily());
                    //浏览器版本
                    userAgentInfo.setBrowserVersion(czUserAgentInfo.getBrowserVersionInfo());
                }


            }
        } catch (IOException e) {
            logger.error("解析useragent异常",e);
        }

        return userAgentInfo;
    }

    /**
     * 用于封装useragent解析后的信息
     */
    public static class UserAgentInfo{


        private String browser;
        private String browserVersion;
        private String system;
        private String systemVersion;

        public UserAgentInfo() {
        }

        public UserAgentInfo(String browser, String browserVersion, String system, String systemVersion) {
            this.browser = browser;
            this.browserVersion = browserVersion;
            this.system = system;
            this.systemVersion = systemVersion;
        }

        public String getBrowser() {
            return browser;
        }

        public void setBrowser(String browser) {
            this.browser = browser;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getSystem() {
            return system;
        }

        public void setSystem(String system) {
            this.system = system;
        }

        public String getSystemVersion() {
            return systemVersion;
        }

        public void setSystemVersion(String systemVersion) {
            this.systemVersion = systemVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browser='" + browser + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", system='" + system + '\'' +
                    ", systemVersion='" + systemVersion + '\'' +
                    '}';
        }
    }
}