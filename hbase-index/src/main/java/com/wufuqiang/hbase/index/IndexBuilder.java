package com.wufuqiang.hbase.index;

import com.wufuqiang.hbase.mapper.IndexMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class IndexBuilder {
    private Configuration configuration;
    private Connection connection;
    private Admin admin ;

    private IndexBuilder(String zkQuorum) throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",zkQuorum);
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    public static IndexBuilder getInstance(String zkQuorum) throws IOException {
        return new IndexBuilder(zkQuorum);
    }

    public void buildIndex(String tableName,String cf,String ... cns) throws IOException, ClassNotFoundException, InterruptedException {
        this.configuration.set("tableName",tableName);
        this.configuration.set("cf",cf);
        this.configuration.setStrings("cns",cns);
        Job job = Job.getInstance(this.configuration);

        job.setJarByClass(IndexBuilder.class);
        job.setMapperClass(IndexMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        Scan scan = new Scan();
        scan.setCaching(1000);
        TableMapReduceUtil.initTableMapperJob(tableName,scan, IndexMapper.class,
                ImmutableBytesWritable.class,Put.class,job);
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
