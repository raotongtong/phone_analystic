package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: LocationDimension
 * @Author: rtt
 * @Date: 2018/9/27 0027 20:42
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class LocationDimension extends BaseDimension{
    private int id = 0;
    private String country = null;
    private String province = null;
    private String city = null;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this.id = id;
        this.country = country;
        this.province = province;
        this.city = city;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        LocationDimension ld = (LocationDimension) o;
        int tmp = this.id - ld.id;
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.country.compareTo(ld.country);
        if(tmp != 0){
            return  tmp;
        }
        tmp = this.province.compareTo(ld.province);
        if(tmp != 0){
            return  tmp;
        }
        return this.city.compareTo(ld.city);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.province);
        dataOutput.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.country = dataInput.readUTF();
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, country, province, city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
