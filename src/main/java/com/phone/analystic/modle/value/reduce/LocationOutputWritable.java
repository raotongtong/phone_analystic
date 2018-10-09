package com.phone.analystic.modle.value.reduce;

import com.phone.analystic.modle.value.StatsOutputValue;
import com.phone.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: OutputWritable
 * @Author: rtt
 * @Date: 2018/9/20 0020 下午 8:55
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class LocationOutputWritable extends OutputWritable {
    private KpiType kpi;
    private int aus ;
    private int sessions;
    private int bounceSession;

    public LocationOutputWritable() {
    }

    public LocationOutputWritable(KpiType kpi, int aus, int sessions, int bounceSession) {
        this.kpi = kpi;
        this.aus = aus;
        this.sessions = sessions;
        this.bounceSession = bounceSession;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpi);
        dataOutput.writeInt(this.aus);
        dataOutput.writeInt(this.sessions);
        dataOutput.writeInt(this.bounceSession);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput,KpiType.class);
        this.aus = dataInput.readInt();
        this.sessions = dataInput.readInt();
        this.bounceSession = dataInput.readInt();
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounceSession() {
        return bounceSession;
    }

    public void setBounceSession(int bounceSession) {
        this.bounceSession = bounceSession;
    }
}
