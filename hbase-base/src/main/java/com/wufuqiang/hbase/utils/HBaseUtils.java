package com.wufuqiang.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public void createNameSpace(String ns){
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e){
          logger.error(String.format("%s命名空间已存在。",ns));
        } catch (IOException e) {
            logger.error("创建命名空间失败。");
            e.printStackTrace();
        }
    }


    /**
     * 创建表
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public void createTable(String tableName,String ... cfs) throws IOException {
        if(cfs.length <= 0){
            logger.error("没有列簇信息。");
            return;
        }
        if(isTableExist(tableName)){
            logger.info("HBase表已经存在。");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {
            //创建列簇描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public void dropTable(String tableName) throws IOException {
        if(!isTableExist(tableName)){
            logger.info(String.format("%s表不存在",tableName));
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }


    public void close(){
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 插入一条数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param value
     * @throws IOException
     */
    public void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 获取rowkey对应的所有数据
     * @param tableName
     * @param rowkey
     * @return
     * @throws IOException
     */
    public List<String[]> getData(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        List<String[]> returnResult = new ArrayList<String[]>();
        for(Cell cell : result.rawCells()){
            String cellRowkey = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            returnResult.add(new String[]{cellRowkey,cf,cn,value});
        }
        table.close();
        return returnResult;
    }

    /**
     * 获取value值
     * @param tableName
     * @param rowkey
     * @param cf
     * @param cn
     * @return
     * @throws IOException
     */
    public String getData(String tableName,String rowkey , String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);
        String value = "";
        for(Cell cell : result.rawCells()){
            value = Bytes.toString(CellUtil.cloneValue(cell));
        }
        table.close();
        return value;
    }

    public List<String[]> scanData(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<String[]> returnResult = new ArrayList<String[]>();
        for(Result result : scanner){
            for(Cell cell : result.rawCells()){
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String[] cellResult  = new String[]{rowKey,cf,cn,value};
                returnResult.add(cellResult);
            }
        }
        table.close();
        return returnResult;
    }

    public void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        table.delete(delete);
        table.close();
    }

}
