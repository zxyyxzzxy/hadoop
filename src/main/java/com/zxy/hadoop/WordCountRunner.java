package com.zxy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ZhouXinyu on 2019/9/4 14:57.
 */
public class WordCountRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf, WordCountRunner.class);
        Job wcjob = Job.getInstance(jobConf);
        wcjob.setJarByClass(WordCountRunner.class);

        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);

        // 设置业务逻辑mapper类输出key和value的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(IntWritable.class);

        // 设置业务逻辑Reduceer类输出key和value的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(IntWritable.class);


        // 指定要处理的数据所在位置
        FileInputFormat.setInputPaths((JobConf) wcjob.getConfiguration(), new Path[]{new Path(args[0])});
        // 指定处理完成的结果保存的位置
        FileOutputFormat.setOutputPath((JobConf) wcjob.getConfiguration(), new Path(args[1]));

        // 向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res? 0 : 1);
    }
}
