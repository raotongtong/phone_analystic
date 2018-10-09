package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: StatsLocationDimension
 * @Author: rtt
 * @Date: 2018/9/27 0027 20:53
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class StatsLocationDimension extends StatsBaseDimension{
    private LocationDimension locationDimension = new LocationDimension();
    private StatsCommonDimention statsCommonDimention = new StatsCommonDimention();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(LocationDimension locationDimension, StatsCommonDimention statsCommonDimention) {
        this.locationDimension = locationDimension;
        this.statsCommonDimention = statsCommonDimention;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.locationDimension.compareTo(other.locationDimension);
        if(tmp != 0){
            return tmp;
        }
        return this.statsCommonDimention.compareTo(other.statsCommonDimention);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        locationDimension.write(dataOutput);
        statsCommonDimention.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        locationDimension.readFields(dataInput);
        statsCommonDimention.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsLocationDimension that = (StatsLocationDimension) o;
        return Objects.equals(locationDimension, that.locationDimension) &&
                Objects.equals(statsCommonDimention, that.statsCommonDimention);
    }

    @Override
    public int hashCode() {

        return Objects.hash(locationDimension, statsCommonDimention);
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    public StatsCommonDimention getStatsCommonDimention() {
        return statsCommonDimention;
    }

    public void setStatsCommonDimention(StatsCommonDimention statsCommonDimention) {
        this.statsCommonDimention = statsCommonDimention;
    }
}
