package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName: PaymentTypeDimension
 * @Author: rtt
 * @Date: 2018/10/4 0004 12:18
 * @Version: 1.0
 * @Description: java类作用描述
 */
public class PaymentTypeDimension extends BaseDimension{
    private int id;
    private String paymentType;

    public PaymentTypeDimension() {
    }

    public PaymentTypeDimension(String paymentType) {
        this.paymentType = paymentType;
    }

    public PaymentTypeDimension(int id, String paymentType) {
        this.id = id;
        this.paymentType = paymentType;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }
        PaymentTypeDimension other = (PaymentTypeDimension)o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }

        return this.paymentType.compareTo(other.paymentType);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.paymentType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.paymentType = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentTypeDimension that = (PaymentTypeDimension) o;
        return id == that.id &&
                Objects.equals(paymentType, that.paymentType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, paymentType);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }
}
