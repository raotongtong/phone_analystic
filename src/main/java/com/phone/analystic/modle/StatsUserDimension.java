package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: StatsUserDimension
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 7:55
 * @Version: 1.0
 * @Description: 可以用于用户模块和浏览器模块的map和reduce阶段输出的key的类型的顶级父类
 */
public class StatsUserDimension extends StatsBaseDimension{
    private BrowserDimension browserDimension = new BrowserDimension();
    private StatsCommonDimention statsCommonDimention = new StatsCommonDimention();


    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimention statsCommonDimention, BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
        this.statsCommonDimention = statsCommonDimention;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        browserDimension.write(dataOutput);
        statsCommonDimention.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        browserDimension.readFields(dataInput);
        statsCommonDimention.readFields(dataInput);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.browserDimension.compareTo(other.browserDimension);
        if(tmp != 0){
            return tmp;
        }
        return this.statsCommonDimention.compareTo(other.statsCommonDimention);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(statsCommonDimention, that.statsCommonDimention) &&
                Objects.equals(browserDimension, that.browserDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(browserDimension,statsCommonDimention);
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    public StatsCommonDimention getStatsCommonDimention() {
        return statsCommonDimention;
    }

    public void setStatsCommonDimention(StatsCommonDimention statsCommonDimention) {
        this.statsCommonDimention = statsCommonDimention;
    }
}
