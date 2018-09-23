package com.phone;

import com.google.common.base.Strings;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
public class MyUserAgentWithRegexUtil {
    public static final Logger logger = Logger.getLogger(MyUserAgentWithRegexUtil.class);

    static UserAgentInfo userAgentInfo = new UserAgentInfo();
    public static UserAgentInfo parserUserAgent(String userAgent){
        if(Strings.isNullOrEmpty(userAgent)){
            logger.warn("解析的userAgent为空");
            return null;
        }

        //220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\"
        //String regex = "^([0-9.]+\\d+) - - \\[(.* \\+\\d+)\\] .+(GET|POST) (.+) (HTTP)\\S+ (\\d+).+\\\"(\\w+).+$";
        //windows  NT  6.1  Chrome  46.0
        String regex = "^M.*\\(([A-Za-z].+)%20([A-Z].+)%20([0-9]//.[0-9]).*\\(.*\\)%20([A-Za-z].+)%2F([1-9].+\\.[0-9])\\..+$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(userAgent);
        String bs = "";
        String ve = "";
        String ws = "";
        if(matcher.find()){
            bs = matcher.group(4);
            ve = matcher.group(5);
            ws = matcher.group(1);
            //这里要了解windows的版本，现在先用window 7来代替
//            String version = matcher.group(2) + matcher.group(3);

        }
        userAgentInfo.setBrowser(bs);
        userAgentInfo.setBrowserVersion(ve);
        userAgentInfo.setSystem(ws);
        userAgentInfo.setSystemVersion("window 7");

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

        public UserAgentInfo() { }

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