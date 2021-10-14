package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyWordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);//工具类把通用的-D属性直接set到conf，会留下commandOptions
		String[] otherArgs = parser.getRemainingArgs();

		// 让框架知道mac提交到Linux运行
		conf.set("mapreduce.app-submission.cross-platform", "true");
//		System.out.println(conf.get("mapreduce.framework.name"));
//		conf.set("mapreduce.framework.name", "local");
//		System.out.println(conf.get("mapreduce.framework.name"));

		Job job = Job.getInstance(conf);
		// 避免Mapper和Reducer不再classpath中。集群方式下，不打jar包分布式节点就不知道应该反射的是哪个类，local下跑不需要配置jar，注掉
		job.setJar("/Users/shuzheng/IdeaProjects/hadoop-practice3/target/hadoop-practice3-1.0-SNAPSHOT.jar");
        job.setJarByClass(MyWordCount.class);

        // Specify various job-specific parameters
        job.setJobName("lisz");

		Path inFile =  new Path(otherArgs[0]); //("/Users/shuzheng/Documents/wc/input");
		TextInputFormat.setInputPaths(job, inFile);
		Path outFile = new Path(otherArgs[1]); //("/Users/shuzheng/Documents/wc/output");
		if (outFile.getFileSystem(conf).exists(outFile)) {
			outFile.getFileSystem(conf).delete(outFile, true);
		}
		TextOutputFormat.setOutputPath(job, outFile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);

		//job.setNumReduceTasks();
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
	}
}
