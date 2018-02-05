package com.nari.adstat.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class AgePartitioner extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numReduceTasks) {
        String nameAgeScore = value.toString().trim();
        String[] fields = nameAgeScore.split("-");
        int age = Integer.parseInt(fields[1]);
        if(age <= 20) {
            return 0;
        } else if(age > 20 && age <= 50) {
            return 1 % numReduceTasks;
        } else {
            return 2 % numReduceTasks;
        }
    }
}
