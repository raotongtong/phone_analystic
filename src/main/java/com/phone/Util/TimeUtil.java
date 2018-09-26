package com.phone.Util;

import com.phone.common.DateEnum;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName TimeUtil
 * @Author rtt
 * @Date $ $
 * @Vesion 1.0
 * @Description 时间工具类
 **/
public class TimeUtil {

    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    /**
     * 判断时间是否有效
     *
     * @param date 用正则表达式判断 yyyy-MM-dd
     * @return
     */
    public static boolean isValidateDate(String date) {
        Matcher matcher = null;
        Boolean res = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (StringUtils.isNotEmpty(date)) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if (matcher != null) {
            res = matcher.matches();
        }
        return res;
    }

    /**
     * 默认获取昨天的日期  yyyy-MM-dd
     *
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DEFAULT_FORMAT);
    }

    /**
     * @param pattern 获取指定格式的昨天的日期
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        //Calendar.DAY_OF_YEAR，一年中的第几天，这里表示你想要获取的当天是这一年中第几天
        //calendar.add():是对哪一个字段进行操作，amount是偏移量，这里是对DAY_OF_YEAR，进行-1操作
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 获取指定日期的前一天
     */
//    public static Date get


    /**
     * 将时间戳转换成默认格式的日期
     *
     * @param time
     * @return
     */
    public static String parseLong2String(long time) {
        return parseLong2String(time, DEFAULT_FORMAT);
    }

    public static String parseLong2String(long time, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }


    /**
     * 将默认的日期格式转换成时间戳
     *
     * @param date
     * @return
     */
    public static long parseString2Long(String date) {
        return parseString2Long(date, DEFAULT_FORMAT);
    }

    public static long parseString2Long(String date, String pattern) {
        Date dt = null;

        try {
            dt = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dt.getTime();
    }

    /**
     * 获取日期信息
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        //给calendar设定一个时间，可以通过getTimeInMillis或获取这个时间(包括年月日等等信息)
        //如果get(Calendar.YEAR)，只会得到设置当前时间的年
        calendar.setTimeInMillis(time);
        if(type.equals(DateEnum.YEAR)){
            return calendar.get(Calendar.YEAR);
        }
        if(type.equals(DateEnum.SEASON)){
            int month = calendar.get(Calendar.MONTH) + 1;
            return month % 3 == 0 ? month / 3 : (month / 3 + 1);
        }
        if(type.equals(DateEnum.MONTH)){
            return calendar.get(Calendar.MONTH) + 1;
        }
        if(type.equals(DateEnum.WEEK)){
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }
        if(type.equals(DateEnum.DAY)){
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if(type.equals(DateEnum.HOUR)){
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw  new RuntimeException("不支持该类型的日期信息获取.type："+type.dateType);
    }

    /**
     * 获取某周第一天时间戳
     * @param time
     * @return
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        //下面设置的会覆盖上面setTimeInMillis中的一部分值
        calendar.set(Calendar.DAY_OF_WEEK,1);//该周的第一天
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTimeInMillis();
    }

    public static void main(String[] args) {
        System.out.println(TimeUtil.isValidateDate("2018-09-20"));
        System.out.println(TimeUtil.isValidateDate("2018-9-20"));
        System.out.println(TimeUtil.getYesterday());
        System.out.println(TimeUtil.getYesterday("yyyy/MM/dd"));
        System.out.println(TimeUtil.parseString2Long("2018-09-20"));
        System.out.println(TimeUtil.parseLong2String(1537372800000L,"yyyy-MM-dd"));
        System.out.println(TimeUtil.getDateInfo(1537372800000L,DateEnum.DAY));
        System.out.println(TimeUtil.getDateInfo(1537372800000L,DateEnum.WEEK));
        System.out.println(TimeUtil.getDateInfo(1537372800000L,DateEnum.SEASON));
        System.out.println(TimeUtil.getDateInfo(1537372800000L,DateEnum.MONTH));
        System.out.println(TimeUtil.getFirstDayOfWeek(1537372800000L));
        System.out.println(TimeUtil.parseLong2String(1537027200000L,"yyyy-MM-dd"));
    }

}