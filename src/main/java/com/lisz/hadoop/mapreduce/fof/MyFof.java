package com.lisz.hadoop.mapreduce.fof;

import com.lisz.hadoop.mapreduce.topn.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyFof {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
//		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
		//conf.set("fs.defaultFS", "local");

		final String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);
		// 仅仅执行map，看输出，debug的时候比较有用
		//job.setNumReduceTasks(0);

		job.setJarByClass(MyFof.class);
		job.setJobName("TopFof");

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
		job.setMapperClass(FofMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// Combiner

		// 2。 reduce
		// shuffle
		// grouping comparator
		// reduce （归并拉取数据都是框架干的）
		job.setReducerClass(FofReducer.class);

		job.waitForCompletion(true);
	}
}
