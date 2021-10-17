package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {
	// 因为map可能被多次调用，定义在外面减少gc，同时，要知道，源码中看到了，map输出的key，value是会发生序列变化，变成字节数组进入buffer的
	private static final TopNKey TOPN_KEY = new TopNKey();
	private static final IntWritable VALUE = new IntWritable();


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//开发习惯，不要过于自信 2019-6-1 22:22:22	1	39
		final String[] split = value.toString().split("\\s+");
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = null;
		try {
			final Date date = sdf.parse(split[0]);
			cal = Calendar.getInstance();
			cal.setTime(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		TOPN_KEY.setYear(cal.get(Calendar.YEAR));
		TOPN_KEY.setMonth(cal.get(Calendar.MONTH) + 1);
		TOPN_KEY.setDay(cal.get(Calendar.DAY_OF_MONTH));
		int temperature = Integer.parseInt(split[3]);
		TOPN_KEY.setTemperature(temperature);

		VALUE.set(temperature);

		context.write(TOPN_KEY, VALUE);
	}
}
