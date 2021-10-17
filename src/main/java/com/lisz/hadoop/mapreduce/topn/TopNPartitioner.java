package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {

	@Override
	public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {
		//分区器设计原则：不能太复杂，因为每一条都要算.Hive里的join key应该就是这里的partition key
		return (topNKey.getYear() + topNKey.getMonth()) % numPartitions;
	}
}
