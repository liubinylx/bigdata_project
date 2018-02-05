package com.nari.adstat;

import com.nari.adstat.bean.AdMetricBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MrPvClickByAreaDayApp {
    static class PvClickMapper extends Mapper<LongWritable, Text, Text, AdMetricBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fields = line.split("\t");
            String areaId = fields[0];
            int viewType = Integer.parseInt(fields[2]);
            String date = fields[3];
            int pv = 0;
            int click = 0;
            if(viewType == 1) {
                pv = 1;
            } else if(viewType == 2) {
                click = 1;
            }

            String keyStr = areaId + "-" + date;
            context.write(new Text(keyStr), new AdMetricBean(pv, click));
        }
    }

    static class PvClickReducer extends Reducer<Text, AdMetricBean, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<AdMetricBean> values, Context context) throws IOException, InterruptedException {
            long pv = 0;
            long click = 0;
            for(AdMetricBean amb : values) {
                pv += amb.getPv();
                click += amb.getClick();
            }

            double clickRatio = (double)click / pv * 100;
            String clickRatioStr = String.format("%.2f", clickRatio).toString() + "%";
            String[] keys = key.toString().split("-");
            String line = keys[1] + "\t" + keys[0] + "\t" + pv + "\t" + click + "\t" + clickRatioStr;

            context.write(new Text(line), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = args[0];
        String outputPath = args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("MrPvClickByAreaDayApp");
        job.setJarByClass(MrPvClickByAreaDayApp.class);
        job.setMapperClass(PvClickMapper.class);
        job.setReducerClass(PvClickReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AdMetricBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
