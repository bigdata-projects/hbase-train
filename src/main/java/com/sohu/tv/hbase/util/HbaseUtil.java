package com.sohu.tv.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.tv.hbase.bean.CustomHbaseModel;

/**
 * hbase接口工具类
 * @author leifu
 * @Date 2016年4月29日
 * @Time 下午4:27:27
 */
public enum HbaseUtil {
    HBASE_BX(HbaseUtil.MAJOR_HBASE_SITE);

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final String MAJOR_HBASE_SITE = "hbase/hbase-site.xml";

    private Configuration config;

    private Connection connection;

    private HbaseUtil(String resource) {
        try {
            this.config = HBaseConfiguration.create();
            this.config.addResource(resource);
            this.connection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public boolean checkConfig() {
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void put(String tableName, CustomHbaseModel customHbaseModel) throws Exception {
        if (customHbaseModel == null) {
            return;
        }
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(customHbaseModel.getRowKey());
            long version = customHbaseModel.getVersion();
            if (version > 0) {
                put.addColumn(customHbaseModel.getFamily(), customHbaseModel.getQualifier(), version,
                        customHbaseModel.getValue());
            } else {
                put.addColumn(customHbaseModel.getFamily(), customHbaseModel.getQualifier(),
                        customHbaseModel.getValue());
            }
            table.put(put);
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    public void batchPut(String tableName, List<CustomHbaseModel> customHbaseModelList) throws Exception {
        Table table = null;
        try {
            List<Put> puts = new ArrayList<Put>();
            for (CustomHbaseModel customHbaseModel : customHbaseModelList) {
                Put put = new Put(customHbaseModel.getRowKey());
                long version = customHbaseModel.getVersion();
                if (version > 0) {
                    put.addColumn(customHbaseModel.getFamily(), customHbaseModel.getQualifier(), version,
                            customHbaseModel.getValue());
                } else {
                    put.addColumn(customHbaseModel.getFamily(), customHbaseModel.getQualifier(),
                            customHbaseModel.getValue());
                }
                puts.add(put);
            }
            table = connection.getTable(TableName.valueOf(tableName));
            table.put(puts);
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    public Result get(String tableName, byte[] rowKey, byte[] family, byte[] qualifier) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey);
            get.addColumn(family, qualifier);
            Result result = table.get(get);
            return result;
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }
    
    public Result get(String tableName, byte[] rowKey, byte[] family) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey);
            get.addFamily(family);
            Result result = table.get(get);
            return result;
        } catch (Exception e) {
            throw e;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    public Configuration getConfig() {
        return config;
    }

    public Connection getConnection() {
        return connection;
    }

}
