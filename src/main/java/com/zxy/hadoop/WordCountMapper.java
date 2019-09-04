package com.zxy.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ZhouXinyu on 2019/9/4 14:47.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到一行数据转换为string
        String line = value.toString();
        // 将这一行且分出各个单词
        String[] words = line.split(" ");
        // 遍历数组，输出<单词， 1>
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
