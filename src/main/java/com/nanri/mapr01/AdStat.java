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

/**
 * 广告统计
 * 按天统计曝光量
 * 升序及降序排列
 */
public class AdStat {
    /**
     * Map程序，读取数据，将曝光量按天为key，1为value写出
     */
    static class AdStatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取数据
            String[] values = value.toString().trim().split("\t");
            //浏览类型，1为曝光，2为点击
            String viewType = values[2].trim();
            String statDat = values[3];
            if("1".equals(viewType)) {//统计曝光量
                context.write(new Text(statDat), new IntWritable(1));
            }
        }
    }

    /**
     * Reduce程序，对每天的Reduce程序加和
     */
    static class AdStatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value : values) {
                count += value.get();
            }
            context.write(new Text(key), new IntWritable(count));
        }
    }

    /**
     * 排序的Map程序，在这里只是简单的调换顺序，以便利用MapReduce的Shuffle进行排序
     */
    static class AdSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\t");
            String statDate = values[0];
            int pv = Integer.parseInt(values[1]);
            context.write(new IntWritable(pv), new Text(statDate));
        }
    }

    /**
     * 调换位置，写出即可
     */
    static class AdSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                String date = value.toString();
                context.write(new Text(date), key);
            }
        }
    }

    //重写IntWritable.Comparator比较器，默认返回正数，升序，倒序返回负数
    public static class IntWritableDescComparator extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
        String jobName = "exposureStat";
        String inputPath = args[0];
        String outputPath = args[1];
        Path tmpPath = new Path("MrPvSortByDayTmp");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(AdStat.class);

        job.setMapperClass(AdStatMapper.class);
        job.setReducerClass(AdStatReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, tmpPath);
        boolean jobPvStatus = job.waitForCompletion(true);
        //排序
        Job jobPvSort = Job.getInstance(conf, "MrPvSortByDay");
        jobPvSort.setJarByClass(AdStat.class);
        jobPvSort.setMapperClass(AdSortMapper.class);
        jobPvSort.setReducerClass(AdSortReducer.class);
        //添加比较器，如果未正序，此比较器不需要添加
        jobPvSort.setSortComparatorClass(IntWritableDescComparator.class);
        jobPvSort.setMapOutputValueClass(Text.class);
        jobPvSort.setMapOutputKeyClass(IntWritable.class);
        jobPvSort.setOutputValueClass(IntWritable.class);
        jobPvSort.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(jobPvSort, tmpPath);
        FileOutputFormat.setOutputPath(jobPvSort, new Path(outputPath));
        if(jobPvStatus) {
            System.exit(jobPvSort.waitForCompletion(true) ? 0:1);
        }

    }
}
