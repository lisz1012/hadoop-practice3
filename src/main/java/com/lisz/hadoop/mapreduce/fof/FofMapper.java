package com.lisz.hadoop.mapreduce.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Text OUTPUT_KEY = new Text();
	private static final IntWritable OUTPUT_VALUE = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		final String[] strs = value.toString().split("\\s+");
		OUTPUT_VALUE.set(0);
		for (int i = 1; i < strs.length; i++) {
			OUTPUT_KEY.set(getFof(strs[0], strs[i]));
			context.write(OUTPUT_KEY, OUTPUT_VALUE);
		}
		OUTPUT_VALUE.set(1);
		for (int i = 1; i < strs.length - 1; i++) {
			for (int j = i + 1; j < strs.length; j++) {
				OUTPUT_KEY.set(getFof(strs[i], strs[j]));
				context.write(OUTPUT_KEY, OUTPUT_VALUE);
			}
		}
	}

	private String getFof(String s1, String s2) {
		if (s1.compareTo(s2) < 0) {
			return s1 + "-" + s2;
		} else {
			return s2 + "-" + s1;
		}
	}
}
