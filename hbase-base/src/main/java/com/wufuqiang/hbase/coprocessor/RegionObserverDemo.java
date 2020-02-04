package com.wufuqiang.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @ author wufuqiang
 **/
public class RegionObserverDemo extends BaseRegionObserver{

    private static byte[] fixedRowkey = Bytes.toBytes("wufuqiang");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        if(Bytes.equals(fixedRowkey,get.getRow())){
            KeyValue kv = new KeyValue(get.getRow(),
                    Bytes.toBytes("time"),Bytes.toBytes("time"),
                    Bytes.toBytes(System.currentTimeMillis()));
            results.add(kv);
        }
    }
}
