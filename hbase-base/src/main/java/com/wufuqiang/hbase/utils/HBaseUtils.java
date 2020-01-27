package com.wufuqiang.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class HBaseUtils {

    private Configuration configuration ;
    private Connection connection;
    private Admin admin ;
    private static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

    private HBaseUtils(String zkQuorum) throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",zkQuorum);
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * 获取HBaseUtils实例
     * @param zkQuorum HBase地址
     * @return
     * @throws IOException
     */
    public static HBaseUtils getInstance(String zkQuorum) throws IOException {
        return new HBaseUtils(zkQuorum);
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void createTable(String tableName,String ... cfs) throws IOException {
        if(cfs.length <= 0){
            logger.error("没有列簇信息。");
            return;
        }
        if(isTableExist(tableName)){
            logger.info("HBase表已经存在。");
            return;
        }

    }


    public void close() throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

}
