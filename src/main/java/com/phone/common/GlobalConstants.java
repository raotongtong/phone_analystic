package com.phone.common;

/**
 * @ClassName GlobalConstants
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 除日志相关的常量
 **/
public class GlobalConstants {

    public static final String RUNNING_DATE = "running_date";

    public static final String DEFAULT_VALUE = "unknow";

    public static final String URL = "jdbc:mysql://node01:3306/report?useUnicode=yes&characterEncoding=utf8";

    public static final String DRIVER = "com.mysql.jdbc.Driver";

    public static final String USER = "root";

    public static final String PASSWORD = "root";

    public static final long DAY_OF_MILISECONDS = 86400000L;//24*60*60*1000

    public static final String YESTERDAY_TOTAL_USER = "yesterday_total_user";

    public static final String NOWDAY_NEW_USER = "nowday_new_user";

    public static final String NOWDAY_NEW_TOTAL_USER = "nowday_new_total_user";

    public static final String STATS_DEVICE_BROWSER_TOTAL_NEW_USERS = "nowday_new_browser_total_user";
}