package com.lisz.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {
	private Configuration conf = null;
	private FileSystem fs = null;

	@Before
	public void conn() {
		conf = new Configuration((true));
		try {
			//fs = FileSystem.get(conf);
			// 参考了 <name>fs.defaultFS</name>
			//        <!--<value>hdfs://hadoop-01:9000</value>-->
			//        <value>hdfs://mycluster</value>
			// 取环境变量 HADOOP_USER_NAME god
			fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "good");
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void mkdir() throws Exception {
		Path path = new Path("/msb01");
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.mkdirs(path);
	}

	@Test
	public void upload() throws Exception {
		BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
		Path outFile = new Path("/msb01/out.txt");
		FSDataOutputStream output = fs.create(outFile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	//in/out交换就成了下载
	@Test
	public void download() throws Exception {
		BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new File("./data/out.txt")));
		Path inFile = new Path("/msb01/out.txt");
		FSDataInputStream input = fs.open(inFile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	@Test
	public void blocks() throws IOException {
		Path file = new Path("/user/god/data.txt");
		FileStatus fileStatus = fs.getFileStatus(file);
		System.out.println(fileStatus.getPermission() + " - " + fileStatus.getOwner() +
				" " + fileStatus.getGroup() + " " + fileStatus.getModificationTime() +
				" " + fileStatus.getPath());
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for (BlockLocation b : fileBlockLocations) {
			System.out.println(b);
		}

		FSDataInputStream in = fs.open(file);
		in.seek(1048576); //分治计算的关键，只读取自己关心的，同时具备距离的概念，优先读取本地的（框架的默认机制）
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
		System.out.println((char) in.readByte());
	}


	@After
	public void close() {
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
