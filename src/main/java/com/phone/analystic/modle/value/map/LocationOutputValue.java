package com.phone.analystic.modle.value.map;

import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: TimeOutputValue
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 2:48
 * @Version: 1.0
 * @Description: Location指标中map端的输出类
 */
public class LocationOutputValue extends StatsOutputValue {
    private String uuid;
    private String sessionId;

    public LocationOutputValue() {
    }

    public LocationOutputValue(String uuid, String sessionId) {
        this.uuid = uuid;
        this.sessionId = sessionId;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(uuid);
        dataOutput.writeUTF(sessionId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uuid = dataInput.readUTF();
        this.sessionId = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationOutputValue that = (LocationOutputValue) o;
        return Objects.equals(uuid, that.uuid) &&
                Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(uuid, sessionId);
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
