package com.phone.analystic.mr.service.impl;

import com.phone.Util.JdbcUtil;
import com.phone.analystic.modle.base.*;
import com.phone.analystic.mr.service.IDimension;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

/**
 * @ClassName: IDimensionImpl
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 9:31
 * @Version: 1.0
 * @Description: 获取基础维度id的实现
 */
public class IDimensionImpl implements IDimension{

    //定义内存缓存：key为维度信息 value是维度id
    //这里要用LinkedHashMap，要在map中进行排序
    private Map<String,Integer> cache = new LinkedHashMap<String,Integer>(){

        /**
         * 这个方法的意思是，当map中存到了5000个key-value后，再加入一个，就把最老的，也就是
         * 第一个给删掉，也就是我们要保证这个map中最多值存5000个key-value
         * 所以存储k-v的集合要有序，这里就用到了LinkedHashMap
         * @param eldest
         * @return
         */
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() >5000;
        }
    };

    /**
     * 我们现在得到的是一个维度对象，这个维度对象中可能有个platformName的属性，那么我们怎么得到
     * platformName对应的ID，那是怎么得到这个ID的？？？所以，
     * 我们首先拿到对象，然后给sql语句赋值，先在数据库中查询一下，有没有这个属性，查找到就把id取出来，
     * 如果查询不到，就插入一条数据，并把新生成的id返回
     *
     * 1、根据维度对象里面的属性值，赋值给对应的sql，然后查询，如果有，则返回对应的维度id
     *      如果查询没有，则先添加到数据库中，bing返回对应的id值
     *
     * @param dimension
     * @return
     * @throws IOException
     * @throws SQLException
     */

    @Override
    public int getDimensionIdByObject(BaseDimension dimension) throws IOException, SQLException {
        Connection conn = null;
        try {
            //构造cachekey
            String cacheKey = buildCacheKey(dimension);
            //查询缓存中是否存在
            if(this.cache.containsKey(cacheKey)){
                return this.cache.get(cacheKey);
            }

            String sqls [] = null;
            if(dimension instanceof KpiDimension){
                sqls= buildKpiSqls(dimension);
            }else if(dimension instanceof PlatformDimention){
                sqls= buildPlatformSqls(dimension);
            }else if(dimension instanceof DateDimension){
                sqls= buildDateSqls(dimension);
            }else if(dimension instanceof BrowserDimension){
                sqls= buildBrowserSqls(dimension);
            }else if(dimension instanceof LocationDimension){
                sqls= buildLocationSqls(dimension);
            }else if(dimension instanceof EventDimension){
                sqls= buildEventSqls(dimension);
            }else if(dimension instanceof CurrencyTypeDimension){
                sqls= buildCurrencyTypeSqls(dimension);
            }else if(dimension instanceof PaymentTypeDimension){
                sqls= buildPaymentTypeSqls(dimension);
            }

            //获取jdbc连接
            conn = JdbcUtil.getConn();
            int id = -1;
            synchronized (this){
                id = this.executSql(conn,sqls,dimension);
            }
            //将结果存储到cache中
            this.cache.put(cacheKey,id);

            return id;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,null,null);
        }
        throw new RuntimeException("获取id失败");
    }

    /**
     * 执行sql
     * @param conn
     * @param sqls
     * @param dimension
     * @return
     */
    private int executSql(Connection conn, String[] sqls, BaseDimension dimension) {
        PreparedStatement psvm = null;
        ResultSet rs = null;
        try {
            //获取查询的sql
            String selectSql = sqls[1];
            psvm = conn.prepareStatement(selectSql);
            this.setArgs(dimension,psvm); //为查询语句赋值
            rs = psvm.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
            //没有查询出来，就插入并返回维度的id
            //这个和上面的代码不能交换顺序，因为要先查询
            psvm = conn.prepareStatement(sqls[0],Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension,psvm); //为插入语句赋值
            psvm.executeUpdate();
            rs = psvm.getGeneratedKeys();
            if(rs.next()){
                return rs.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(null,psvm,rs);
        }
        return -1;
    }

    //赋值参数
    private void setArgs(BaseDimension dimension, PreparedStatement psvm) {
        try {
            int i=0;
            if(dimension instanceof KpiDimension){
                KpiDimension kpi = (KpiDimension)dimension;
                psvm.setString(++i,kpi.getKpiName());
            }else if(dimension instanceof DateDimension){
                DateDimension date = (DateDimension)dimension;
                psvm.setInt(++i,date.getYear());
                psvm.setInt(++i,date.getSeason());
                psvm.setInt(++i,date.getMonth());
                psvm.setInt(++i,date.getWeek());
                psvm.setInt(++i,date.getDay());
                psvm.setString(++i,date.getType());
                psvm.setDate(++i,new Date(date.getCalendar().getTime()));
            }else if(dimension instanceof PlatformDimention){
                PlatformDimention platform = (PlatformDimention)dimension;
                psvm.setString(++i,platform.getPlatformName());
            }else if(dimension instanceof BrowserDimension){
                BrowserDimension browser = (BrowserDimension)dimension;
                psvm.setString(++i,browser.getbrowserName());
                psvm.setString(++i,browser.getbrowserVersion());
            }else if(dimension instanceof LocationDimension){
                LocationDimension location = (LocationDimension)dimension;
                psvm.setString(++i,location.getCountry());
                psvm.setString(++i,location.getProvince());
                psvm.setString(++i,location.getCity());
            }else if(dimension instanceof EventDimension){
                EventDimension event = (EventDimension)dimension;
                psvm.setString(++i,event.getCategory());
                psvm.setString(++i,event.getAction());
            }else if(dimension instanceof CurrencyTypeDimension){
                CurrencyTypeDimension currency = (CurrencyTypeDimension)dimension;
                psvm.setString(++i,currency.getCurrencyName());
            }else if(dimension instanceof PaymentTypeDimension){
                PaymentTypeDimension payment = (PaymentTypeDimension)dimension;
                psvm.setString(++i,payment.getPaymentType());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //数据库表的数据，在这里生成，且都是原数据中的数据
    private String[] buildBrowserSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_browser(browser_name,browser_version) values(?,?)";
        String selectSql = "select id from dimension_browser where browser_name=? and browser_version=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        String selectSql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        return new String[]{insertSql,selectSql};
    }


    private String[] buildPlatformSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_platform(platform_name) values(?)";
        String selectSql = "select id from dimension_platform where platform_name=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildKpiSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_kpi(kpi_name) values(?)";
        String selectSql = "select id from dimension_kpi where kpi_name=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildLocationSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_location(country,province,city) values(?,?,?)";
        String selectSql = "select id from dimension_location where country=? and province=? and city=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_event(category,action) values(?,?)";
        String selectSql = "select id from dimension_event where category=? and action=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildCurrencyTypeSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_currency_type(currency_name) values(?)";
        String selectSql = "select id from dimension_currency_type where currency_name=?";
        return new String[]{insertSql,selectSql};
    }

    private String[] buildPaymentTypeSqls(BaseDimension dimension) {
        String insertSql = "insert into dimension_payment_type(payment_type) values(?)";
        String selectSql = "select id from dimension_payment_type where payment_type=?";
        return new String[]{insertSql,selectSql};
    }

    /**
     * 构建维度key
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        //这里key的命名，可以自己写，只要key不重复即可
        if(dimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append(browser.getbrowserName());
            sb.append(browser.getbrowserVersion());
            //browser_IE8.0
        }else if(dimension instanceof KpiDimension){
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append(kpi.getKpiName());
            //kpi_new_user
        }else if(dimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension) dimension;
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        }else if(dimension instanceof PlatformDimention){
            sb.append("platform_");
            PlatformDimention platform = (PlatformDimention) dimension;
            sb.append(platform.getPlatformName());
            //kpi_new_user
        } else if(dimension instanceof LocationDimension){
            sb.append("location_");
            LocationDimension location = (LocationDimension) dimension;
            sb.append(location.getCountry());
            sb.append(location.getProvince());
            sb.append(location.getCity());
            //kpi_new_user
        }else if(dimension instanceof EventDimension){
            sb.append("event_");
            EventDimension eventDimension = (EventDimension) dimension;
            sb.append(eventDimension.getCategory());
            sb.append(eventDimension.getAction());
        }else if(dimension instanceof CurrencyTypeDimension){
            sb.append("currency_type_");
            CurrencyTypeDimension currencyTypeDimension = (CurrencyTypeDimension) dimension;
            sb.append(currencyTypeDimension.getCurrencyName());
        }else if(dimension instanceof PaymentTypeDimension){
            sb.append("payment_type_");
            PaymentTypeDimension paymentTypeDimension = (PaymentTypeDimension) dimension;
            sb.append(paymentTypeDimension.getPaymentType());
        }
        return sb.toString();
    }
}
