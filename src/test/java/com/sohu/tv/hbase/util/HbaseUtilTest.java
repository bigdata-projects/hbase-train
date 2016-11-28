package com.sohu.tv.hbase.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.tv.hbase.bean.CustomHbaseModel;

/**
 * hbase相关api测试
 * 
 * @author leifu
 * @Date 2016年4月29日
 * @Time 下午4:35:38
 */
public class HbaseUtilTest {

    private Logger logger = LoggerFactory.getLogger(HbaseUtilTest.class);

    public final String tableName = "video_test";
    private final byte[] family = Bytes.toBytes("p");

    @Test
    public void testCheckConfig() {
        boolean success = HbaseUtil.HBASE_TEST.checkConfig();
        logger.info("check config result is {}", success);
    }
    
    
    @Test
    public void testCreateTable() {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cf = new HColumnDescriptor(family);
//        cf.setCompactionCompressionType(Compression.Algorithm.LZO);
//        cf.setCompressionType(Compression.Algorithm.LZO);
//        cf.setMaxVersions(1);
//        cf.setMinVersions(0);
//        //布隆过滤器
//        cf.setBloomFilterType(BloomType.ROW);
        tableDescriptor.addFamily(cf);
        HbaseUtil.HBASE_TEST.createTableByAutoSplit(tableDescriptor);
        
    }

    @Test
    public void testPut() throws Exception {
        byte[] rowKey = Bytes.toBytes("vid:1");
        byte[] qualifier = Bytes.toBytes("info");
        byte[] value = Bytes.toBytes("testInfo");
        HbaseUtil.HBASE_TEST.put(tableName, CustomHbaseModel.getPutModel(rowKey, family, qualifier, value));
    }

    @Test
    public void testBatchPut() throws Exception {
        List<CustomHbaseModel> customHbaseModelList = new ArrayList<CustomHbaseModel>();
        for (int i = 1; i <= 10; i++) {
            byte[] rowKey = Bytes.toBytes("vid:" + i);
            byte[] qualifier = Bytes.toBytes("name" + i);
            byte[] value = Bytes.toBytes("testValue" + i);
            customHbaseModelList.add(CustomHbaseModel.getPutModel(rowKey, family, qualifier, value));
        }
        HbaseUtil.HBASE_TEST.batchPut(tableName, customHbaseModelList);
    }

    @Test
    public void testGetColumn() throws Exception {
        byte[] rowKey = Bytes.toBytes("vid:2");
        byte[] qualifier = Bytes.toBytes("name");
        Result result = HbaseUtil.HBASE_TEST.get(tableName, rowKey, family, qualifier);
        printResult(result);
    }

    @Test
    public void testGetFamily() throws Exception {
        byte[] rowKey = Bytes.toBytes("vid:2");
        Result result = HbaseUtil.HBASE_TEST.get(tableName, rowKey, family);
        printResult(result);
    }

    @Test
    public void testDel() throws Exception {
        byte[] rowKey = Bytes.toBytes("vid:2");
        byte[] qualifier = Bytes.toBytes("name");
        HbaseUtil.HBASE_TEST.delete(tableName, rowKey, family, qualifier);
    }

    private void printResult(Result result) {
        if (result == null) {
            return;
        }
        String resultRowKey = Bytes.toString(result.getRow());
        logger.info("resultRowKey: {}", resultRowKey);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] rowKeyByte = CellUtil.cloneRow(cell);
            byte[] familyByte = CellUtil.cloneFamily(cell);
            byte[] qualifierByte = CellUtil.cloneQualifier(cell);
            byte[] valueByte = CellUtil.cloneValue(cell);
            logger.info("rowkey:{}, family:{}, column:{}, value:{}", Bytes.toString(rowKeyByte),
                    Bytes.toString(familyByte), Bytes.toString(qualifierByte), Bytes.toString(valueByte));
        }
    }

}
