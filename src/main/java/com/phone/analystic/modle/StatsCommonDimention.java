package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimention;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: StatsCommonDimention
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 7:36
 * @Version: 1.0
 * @Description: 公共维度类
 */
public class StatsCommonDimention extends StatsBaseDimension{
    public DateDimension dateDimension = new DateDimension();
    public PlatformDimention platformDimention = new PlatformDimention();
    public KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimention() {
    }

    public StatsCommonDimention(DateDimension dateDimension, PlatformDimention platformDimention, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimention = platformDimention;
        this.kpiDimension = kpiDimension;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.dateDimension.write(dataOutput);
        this.platformDimention.write(dataOutput);
        this.kpiDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateDimension.readFields(dataInput);
        this.platformDimention.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        StatsCommonDimention other = (StatsCommonDimention) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.platformDimention.compareTo(other.platformDimention);
        if(tmp != 0){
            return tmp;
        }
        return this.kpiDimension.compareTo(other.kpiDimension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsCommonDimention that = (StatsCommonDimention) o;
        return Objects.equals(dateDimension, that.dateDimension) &&
                Objects.equals(platformDimention, that.platformDimention) &&
                Objects.equals(kpiDimension, that.kpiDimension);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dateDimension, platformDimention, kpiDimension);
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimention getPlatformDimention() {
        return platformDimention;
    }

    public void setPlatformDimention(PlatformDimention platformDimention) {
        this.platformDimention = platformDimention;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
