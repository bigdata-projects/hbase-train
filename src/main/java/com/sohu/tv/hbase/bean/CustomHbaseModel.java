package com.sohu.tv.hbase.bean;

import java.util.Arrays;

/**
 * 自定义hbase模型bean
 * 
 * @author leifu
 * @Date 2016年5月3日
 * @Time 下午2:14:13
 */
public class CustomHbaseModel {

    private byte[] rowKey;

    private byte[] family;

    private byte[] qualifier;

    private byte[] value;

    private long version;

    private CustomHbaseModel(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    private CustomHbaseModel(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long version) {
        super();
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.version = version;
    }

    public static CustomHbaseModel getPutModel(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) {
        return new CustomHbaseModel(rowKey, family, qualifier, value);
    }

    public static CustomHbaseModel getPutModel(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value, long version) {
        return new CustomHbaseModel(rowKey, family, qualifier, value, version);
    }
    
    public static CustomHbaseModel getGetModel(byte[] rowKey, byte[] family, byte[] qualifier, long version) {
        return new CustomHbaseModel(rowKey, family, qualifier, null, version);
    }
    
    public static CustomHbaseModel getGetModel(byte[] rowKey, byte[] family, byte[] qualifier) {
        return new CustomHbaseModel(rowKey, family, qualifier, null, 0);
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "CustomHbaseModel [rowKey=" + Arrays.toString(rowKey) + ", family=" + Arrays.toString(family)
                + ", qualifier=" + Arrays.toString(qualifier) + ", value=" + Arrays.toString(value) + ", version="
                + version + "]";
    }
    
    
    

}
