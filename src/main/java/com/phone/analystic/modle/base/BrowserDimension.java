package com.phone.analystic.modle.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName BrowserDimension
 * @Author rtt
 * @Date $ $
 * @Vesion 1.0
 * @Description
 **/
public class BrowserDimension extends BaseDimension{
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDimension() {
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id, String browserName, String browserVersion) {
        this(browserName,browserVersion);
        this.id = id;
    }

    //否建浏览器的维度集合对象
    public static BrowserDimension getInstance(String browserName,String browserVersion){
        if(StringUtils.isEmpty(browserName)){
            browserName = browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        return new BrowserDimension(browserName,browserVersion);
    }

    public String getbrowserName() {
        return browserName;
    }

    public void setbrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getbrowserVersion() {
        return browserVersion;
    }

    public void setbrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }


    //构建浏览器的维度集合对象

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(browserName);
        dataOutput.writeUTF(browserVersion);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.browserName = dataInput.readUTF();
        this.browserVersion = dataInput.readUTF();
    }


    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if(tmp != 0){
            return tmp;
        }
        return this.browserVersion.compareTo(browserVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browserName, browserVersion);
    }
}