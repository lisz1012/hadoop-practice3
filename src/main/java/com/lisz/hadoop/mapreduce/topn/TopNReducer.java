package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {
	private static final Text OUTPUT_KEY = new Text();
	private static final IntWritable OUTPUT_VALUE = new IntWritable();

	// 对着values进行迭代的时候key会发生变化！因为values.next()的时候，回调用nextKeyValue(), 其中调用了
	// key = keyDeserializer.deserialize(key);  和 writable.readFields(dataIn); 每次从六里面读取下一条的key并赋值
	// 又因为这里是引用传递，所以key这个引用指向的内容在随着dataIn的被读取而不断变化
	@Override
	protected void reduce(TopNKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		/*
		  1970-6-4 33 33
		  1970-6-4 32 32
		  1970-6-22 31 31
		  1970-6-4 22 22
		 */
		int year = key.getYear();
		int month = key.getMonth();
		int day = key.getDay();
		OUTPUT_KEY.set(year + "-" + month + "-" + day);
		OUTPUT_VALUE.set(key.getTemperature());
		context.write(OUTPUT_KEY, OUTPUT_VALUE);
		for (IntWritable i : values) {
			if (key.getDay() != day) {
				OUTPUT_KEY.set(year + "-" + month + "-" + key.getDay());
				OUTPUT_VALUE.set(key.getTemperature());
				context.write(OUTPUT_KEY, OUTPUT_VALUE);
				return;
			}
		}
	}
}
