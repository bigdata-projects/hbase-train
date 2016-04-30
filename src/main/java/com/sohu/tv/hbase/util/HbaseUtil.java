/*
 * Copyright (c) 2013 Sohu TV. All rights reserved.
 */
package com.sohu.tv.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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

    public Configuration getConfig() {
        return config;
    }

    public Connection getConnection() {
        return connection;
    }

}
