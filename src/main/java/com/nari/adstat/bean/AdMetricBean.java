package com.nari.adstat.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdMetricBean implements Writable {
    private long click;
    private long pv;

    //反序列化时需要调用空构造函数
    public AdMetricBean(){}

    public AdMetricBean(long pv, long click) {
        this.pv = pv;
        this.click = click;
    }

    public long getClick() {
        return click;
    }

    public void setClick(long click) {
        this.click = click;
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     * 序列化时顺序写出
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pv);
        dataOutput.writeLong(click);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     * 反序列化的顺序和序列化的顺序一致
     */
    public void readFields(DataInput dataInput) throws IOException {
        //由于在序列化时先写入pv，所以在反序列化时先拿出pv
        pv = dataInput.readLong();
        click = dataInput.readLong();
    }

}
