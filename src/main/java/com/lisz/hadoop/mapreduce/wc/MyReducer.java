package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	// 相同的key为一组，这一组数据调用一次reduce
	// hello 1
	// hello 1
	// hello 1
	// hello 1
	// values就是一堆1
	public void reduce(Text key, Iterable<IntWritable> values,
	                   Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) { // 相对于Reducer.run()中的while这个while是内层循环，用了假@迭代器，而后者才用的真@迭代器
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
    }
}
