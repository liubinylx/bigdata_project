package com.nanri.mapr01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApp {
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取数据，将数据转换成字符串
            String line = value.toString().trim();
            //拆分数据
            if("".equals(line)){
                return;
            }
            String[] words = line.split(" ");
            for(String word : words) {
                //输出需要序列化写出
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 输入：k:v1, v2, v3
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            if("".equals(key.toString().trim())) {
                return;
            }
            for(IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String jobName = args[0];
        String inputPath = args[1];
        String outputPath = args[2];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置作业名称
        job.setJobName(jobName);
        //设置入口类
        job.setJarByClass(WordCountApp.class);
        //mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置mapper阶段的输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reducer阶段的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 作业完成退出
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

