package com.wufuqiang.hbase.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ author wufuqiang
 **/
public class IndexMapper extends TableMapper<ImmutableBytesWritable,Put> {

    Map<byte[],ImmutableBytesWritable> indexes = new HashMap<byte[],ImmutableBytesWritable>();
    private String cf ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String tableName = configuration.get("tableName");
        cf = configuration.get("cf");
        String[] cns = configuration.getStrings("cns");
        for (String cn : cns) {
            indexes.put(Bytes.toBytes(cn),
                    new ImmutableBytesWritable(
                            Bytes.toBytes(
                                    String.format("%s-%s",tableName,cn))));
        }

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        for(Map.Entry<byte[],ImmutableBytesWritable> entry:indexes.entrySet()){
            ImmutableBytesWritable indexTableName = entry.getValue();
            byte[] val = value.getValue(Bytes.toBytes(cf), entry.getKey());
            if(val != null){
                Put put = new Put(val);
                put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("rowkey"),key.get());
                context.write(indexTableName,put);
            }
        }
    }
}
