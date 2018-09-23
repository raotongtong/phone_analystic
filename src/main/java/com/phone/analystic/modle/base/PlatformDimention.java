package com.phone.analystic.modle.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import javax.print.DocFlavor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 平台维度类
 */
public class PlatformDimention extends BaseDimension {
    private int id;
    private String platformName;


    public PlatformDimention() {
    }

    public PlatformDimention(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimention(int id, String platformName) {
        this.id = id;
        this.platformName = platformName;
    }

    public static PlatformDimention getInstance(String platformName){
        String pl = StringUtils.isEmpty(platformName)? GlobalConstants.DEFAULT_VALUE:platformName;
        return new PlatformDimention(pl);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return  0;
        }
        PlatformDimention bd = (PlatformDimention) o;
        int tmp = this.id - bd.id;
        if(tmp != 0){
            return  tmp;
        }
      return this.platformName.compareTo(bd.platformName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.platformName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimention that = (PlatformDimention) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}
