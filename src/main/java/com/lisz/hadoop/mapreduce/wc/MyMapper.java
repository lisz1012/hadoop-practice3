package com.lisz.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	// hadoop中，它是一个分布式，数据要做序列化和反序列化，hadoop有自己一它可以序列化和反序列化
	// 或者自己开发类型，实现序列化、反序列化接口、比较器接口
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // key是每一个字符串自己第一个字符面向源文件的偏移量， value是那个字符串
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one); // word和one变成了字节数组保存了之后，才做后依次循环。word和one定义在在里面会有GC压力，因为一般数据量会比较大，循环较多。
        }
    }
}
