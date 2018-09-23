package com.phone.analystic.modle.value.map;

import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: TimeOutputValue
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 2:48
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class TimeOutputValue extends StatsOutputValue {
    private String id; //对id的泛指，可以使uuid，可以使umid，可以是sessionId
    private long time; //时间戳

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time = dataInput.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
