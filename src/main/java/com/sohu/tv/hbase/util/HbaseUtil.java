package com.sohu.tv.hbase.util;

import static org.apache.hadoop.hbase.util.Bytes.toBigDecimal;
import static org.apache.hadoop.hbase.util.Bytes.toBoolean;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toDouble;
import static org.apache.hadoop.hbase.util.Bytes.toFloat;
import static org.apache.hadoop.hbase.util.Bytes.toInt;
import static org.apache.hadoop.hbase.util.Bytes.toLong;
import static org.apache.hadoop.hbase.util.Bytes.toShort;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sohu.tv.hbase.bean.CustomHbaseModel;

/**
 * hbase接口工具类
 * 
 * @author leifu
 * @Date 2016年4月29日
 * @Time 下午4:27:27
 */
public enum HbaseUtil {
    HBASE_TEST(HbaseUtil.MAJOR_HBASE_SITE);

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
    
    
    public boolean createTableByAutoSplit(HTableDescriptor table) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            admin.createTable(table);
            return true;
        } catch (TableExistsException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return false;
    }
    
    public boolean createTableByManualSplit(HTableDescriptor table, byte[][] splits) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return false;
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

    public void delete(String tableName, byte[] rowKey, byte[] family, byte[] qualifier) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(rowKey);
            delete.addColumn(family, qualifier);
            table.delete(delete);
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

    /**
     * 类型转换成字节
     * 
     * @param obj
     * @return
     */
    public byte[] toByte(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Boolean) {
            return toBytes((Boolean) obj);
        } else if (obj instanceof Double) {
            return toBytes((Double) obj);
        } else if (obj instanceof Float) {
            return toBytes((Float) obj);
        } else if (obj instanceof Short) {
            return toBytes((Short) obj);
        } else if (obj instanceof String) {
            return toBytes((String) obj);
        } else if (obj instanceof BigDecimal) {
            return toBytes((BigDecimal) obj);
        } else if (obj instanceof Long) {
            return toBytes((Long) obj);
        } else if (obj instanceof Integer) {
            return toBytes((Integer) obj);
        }
        throw new IllegalArgumentException("type no support!");
    }

    /**
     * 字节转换成指定类型
     * 
     * @param obj
     * @param clazz
     * @return
     */
    public Object toType(byte[] obj, Class<?> clazz) {
        if (obj == null || obj.length == 0) {
            return null;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return toBoolean(obj);
        } else if (clazz == Double.class || clazz == double.class) {
            return toDouble(obj);
        } else if (clazz == Float.class || clazz == float.class) {
            return toFloat(obj);
        } else if (clazz == Integer.class || clazz == int.class) {
            return toInt(obj);
        } else if (clazz == String.class) {
            return Bytes.toString(obj);
        } else if (clazz == BigDecimal.class) {
            return toBigDecimal(obj);
        } else if (clazz == Long.class || clazz == long.class) {
            return toLong(obj);
        } else if (clazz == Short.class || clazz == short.class) {
            return toShort(obj);
        }
        throw new IllegalArgumentException("type no support!");
    }

    public Configuration getConfig() {
        return config;
    }

    public Connection getConnection() {
        return connection;
    }

}
