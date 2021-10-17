package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

public class MyTopN {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
//		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
		//conf.set("fs.defaultFS", "local");

		final String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);

		job.setJarByClass(MyTopN.class);
		job.setJobName("TopN");

		// 客户端规划的时候将join的右表cache到mapTask出现的节点上. mapreduce.framework.name 一定不能是local，框架要发送文件，集群运行
		job.addCacheFile(new Path("/data/topn/dict").toUri());

		//关注client代码的梳理，这个写明白了，其实也就真的知道这个作业的开发原理了
		// 1。 map
		// in/output
		TextInputFormat.addInputPath(job, new Path(other[0]));
		final Path outPath = new Path(other[1]);
		if (outPath.getFileSystem(conf).exists(outPath)) {
			outPath.getFileSystem(conf).delete(outPath, true);
		}
		TextOutputFormat.setOutputPath(job, outPath);
		// key
		// map
		job.setMapperClass(TopNMapper.class);
		job.setOutputKeyClass(TopNKey.class);
		job.setOutputValueClass(IntWritable.class);
		// partitioner 分区 > 分组，甚至可以按照年分区。分区起的潜台词只要满足：相同的key获得相同的分区号
		job.setPartitionerClass(TopNPartitioner.class);
		// sort 比较器: 年、月、温度，且温度倒序
		job.setSortComparatorClass(TopNKeyComparator.class);
		// Combiner

		// 2。 reduce
		// shuffle
		// grouping comparator
		job.setGroupingComparatorClass(TopNGroupingComparator.class);
		// reduce （归并拉取数据都是框架干的）
		job.setReducerClass(TopNReducer.class);

		job.waitForCompletion(true);
	}

}
