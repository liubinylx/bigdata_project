package com.nari.adstat.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 求不同年龄，不同性别的最高分
 * 本例数据在/data/scoredata中
 */
public class ScoreStatByAgeGenderApp {
    static class ScoreStatMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\t");
            String outputValue = values[0] + "-" + values[1] + "-" + values[3];
            context.write(new Text(values[2]), new Text(outputValue));
        }
    }

    static class ScoreStatReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxScore = 0;
            String maxAge = "";
            String maxName = "";
            String gender = "";
            for(Text value : values) {
                String valueStr = value.toString().trim();
                String[] valueArr = valueStr.split("-");
                int currScore = Integer.parseInt(valueArr[2]);
                if(maxScore < currScore) {
                    maxScore = currScore;
                    maxAge = valueArr[1];
                    maxName = valueArr[0];
                    gender = key.toString();
                }
            }
            String outValue = maxName + "\t" + maxAge + "\t" + maxScore;
            context.write(new Text(gender), new Text(outValue));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = args[0];
        String outputPath = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("MaxScoreStat");
        job.setJarByClass(ScoreStatByAgeGenderApp.class);
        job.setMapperClass(ScoreStatMapper.class);
        job.setReducerClass(ScoreStatReducer.class);

        //设置自定义的partitioner
        job.setPartitionerClass(AgePartitioner.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
