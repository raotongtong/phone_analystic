package com.phone.common;

/**
 * 整个日志的常量类
 */
public class Constants {

    /**
     * 事件的枚举
     *  枚举相当于定义常量，LAUNCH= "e_1"，只不过枚举可以给常量加一个范围
     *  前面的LAUNCH就是一个枚举 通过LAUNCH.id 可以获取该枚举的id的值，其他的值同上
     *
     *
     *  枚举类型其实是整型,它的作用一是让数值看起来更直观，二是圈定一个范围,是数值被限定在这个范围中。
     */
    public enum EventEnum{
        //下面这几行代码必须写在最前面
        //1为id，通过1可以识别LAUNCH，"lanuch event"只是一个描述，可以删除
        //"e_1" 为别名，通过别名也可以找到LAUNCH
        LANUCH(1,"lanuch event","e_1"),
        PAGEVIEW(2,"pageview event","e_pv"),
        EVENT(3,"event name","e_e"),
        CHARGEREQUEST(4,"charge request event","e_crt"),
        CHARGESUCCESS(5,"charge success","e_cs"),
        CHARGEREFUND(6,"charge refund","e_cr")
        ;

        public int id;
        public String name;
        public String alias;

        //构造器,前面不能加public，规定是这样的
        EventEnum(int id,String name,String alias){
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

    }

    //根据名称获取枚举
    public static EventEnum valueOfAlias(String alia){
        //在枚举中，有values方法，可以将枚举一个个取出来(大写的就是枚举,后面是他的别名)
        for(EventEnum event : EventEnum.values()){
            //取出来的alias，和传入的值进行比较，来取出对应的枚举
            if(event.alias.equals(alia)){
                return event;
            }

        }
        throw new RuntimeException("该alias没有对应的枚举.alias" +alia);
    }

    public static final String LOG_VERSION = "ver";

    public static final String LOG_SERVER_TIME = "s_time";

    public static final String LOG_EVENT_NAME = "en";

    public static final String LOG_UUID = "u_ud";

    public static final String LOG_MEMBER_ID = "u_mid";

    public static final String LOG_SESSION_ID = "u_sd";

    public static final String LOG_CLIENT_TIME = "c_time";

    public static final String LOG_LANGUAGE = "l";

    public static final String LOG_USERAGENT = "b_iev";

    public static final String LOG_RESOLUTION = "b_rst";

    public static final String LOG_CURRENT_URL = "p_url";

    public static final String LOG_PREFFER_URL = "p_ref";

    public static final String LOG_TITLE = "tt";

    public static final String LOG_PLATFORM = "pl";

    public static final String LOG_IP = "ip";


    /**
     * 和订单相关
     */
    public static final String LOG_ORDER_ID = "oid";

    public static final String LOG_ORDER_NAME = "on";

    public static final String LOG_CURRENCY_AMOUTN = "cua";

    public static final String LOG_CURRENCY_TYPE = "cut";

    public static final String LOG_PAYMENT_TYPE = "pt";


    /**
     * 事件相关
     * 点击：点击事件 category转发
     * 下单：下单事件
     */
    public static final String LOG_EVENT_CATEGORY = "ca";

    public static final String LOG_EVENT_ACTION = "ac";

    public static final String LOG_EVENT_KV = "kv_";

    public static final String LOG_EVENT_DURATION = "du";

    /**
     * 浏览器相关
     */

    public static final String LOG_BROWSER_NAME = "browserName";

    public static final String LOG_BROWSER_VERSION = "browserVersion";

    public static final String LOG_OS_NAME = "osName";

    public static final String LOG_OS_VERSION = "osVersion";
    /**
     * 地域相关
     */

    public static final String LOG_COUNTRY = "country";

    public static final String LOG_PROVINCE = "province";

    public static final String LOG_CITY = "city";
}
