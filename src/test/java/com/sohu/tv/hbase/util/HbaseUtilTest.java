package com.sohu.tv.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author leifu
 * @Date 2016年4月29日
 * @Time 下午4:35:38
 */
public class HbaseUtilTest {

    public TableName tableName = TableName.valueOf("video_test");

    private Connection connection;

    private final byte[] family = Bytes.toBytes("p");

    public HbaseUtilTest() {
        connection = HbaseUtil.HBASE_BX.getConnection();
    }
    
    @Test
    public void testCheckConfig() {
        boolean success = HbaseUtil.HBASE_BX.checkConfig();
        System.out.println("check config result is " + success);
    }

    @Test
    public void testPut() throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            byte[] rowKey = Bytes.toBytes("vid:1");
            byte[] qualifier = Bytes.toBytes("name");
            byte[] value = Bytes.toBytes("hahahahahha");
            Put put = new Put(rowKey);
            put.addColumn(family, qualifier, value);
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testBatchPut() throws IOException {
        Table table = null;
        try {
            List<Put> puts = new ArrayList<Put>();
            table = connection.getTable(tableName);
            for (int i = 0; i < 100; i++) {
                byte[] rowKey = Bytes.toBytes("vid:" + i);
                byte[] qualifier = Bytes.toBytes("name" + i);
                byte[] value = Bytes.toBytes("testValue" + i);
                Put put = new Put(rowKey);
                put.addColumn(family, qualifier, value);
                puts.add(put);
            }
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Test
    public void testGetColumn() throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            byte[] rowKey = Bytes.toBytes("vid:1");
            Get get = new Get(rowKey);
            byte[] column = Bytes.toBytes("name");
            get.addColumn(family, column);
            Result result = table.get(get);
            if (!result.isEmpty()) {
                byte[] value = result.getValue(family, column);
                System.out.println(Bytes.toString(value));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
    
}
