package com.phone.common;

/**
 * @ClassName KpiType
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 统计指标的枚举
 **/

/**
 * 这个枚举之后还会修改的，因为现在只是统计了new_user指标和browser_new_user,后面还有很多kpi指标，到时候，都要在这里添加
 */
public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member")
    ;

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取对应的指标
     * @param name
     * @return
     */
    public static KpiType valueOfKpiName(String name){
        for (KpiType kpi : values()){
            if(kpi.kpiName.equals(name)){
                return kpi;
            }
        }
        return null;
    }

}