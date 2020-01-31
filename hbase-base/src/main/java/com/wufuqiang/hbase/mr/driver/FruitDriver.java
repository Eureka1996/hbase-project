package com.wufuqiang.hbase.mr.driver;

import com.wufuqiang.hbase.mr.mapper.FruitMapper;
import com.wufuqiang.hbase.mr.reducer.FruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ author wufuqiang
 **/
public class FruitDriver implements Tool{

    private Configuration configuration ;

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FruitDriver.class);

        //设置Mapper、Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer类
        TableMapReduceUtil.initTableReducerJob(args[1],FruitReducer.class,job);

        FileInputFormat.setInputPaths(job,new Path(args[0]));

        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public void setConf(Configuration configuration) {
        this.configuration = configuration;

    }

    public Configuration getConf() {
        return null;
    }

    public static void main(String[]  args) throws Exception {

        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf,new FruitDriver(),args);

    }
}
