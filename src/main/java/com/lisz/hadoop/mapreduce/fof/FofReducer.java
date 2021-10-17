package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final IntWritable OUTPUT_VALUE = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int commonFriendsCount = 0;
		for (IntWritable i : values) {
			if (i.get() == 0) {
				return;
			}
			commonFriendsCount += i.get();
		}
		OUTPUT_VALUE.set(commonFriendsCount);
		context.write(key, OUTPUT_VALUE);
	}
}
