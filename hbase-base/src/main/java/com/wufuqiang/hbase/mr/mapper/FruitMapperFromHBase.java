package com.wufuqiang.hbase.mr.mapper;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class FruitMapperFromHBase extends TableMapper<ImmutableBytesWritable,Put>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Put put = new Put(key.get());
        for(Cell cell : value.rawCells()){
            String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
            if("name".equals(cn)){
                put.add(cell);
                context.write(key,put);
            }
        }
    }
}
