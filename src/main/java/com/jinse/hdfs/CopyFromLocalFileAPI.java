package com.jinse.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class CopyFromLocalFileAPI {

	//1.文件上传
	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		//1.获取fs对象
		Configuration conf=new Configuration();
		//设置副本的数量 代码中优先级最高
		conf.set("fs.replication", "2");
		FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
		
		//2.执行上传API
		fs.copyFromLocalFile(new Path("E:/Kugou/Cake.mp3"), new Path("/Cake.mp3"));
		//3.关闭资源
		fs.close();
	}
	//2.文件下载
	
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		//1.获取对象
		FileSystem fs= FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
		//2.执行下载操作
		fs.copyToLocalFile(new Path("/hadoop.docx"), new Path("D:/hadoop.docx"));
		//3.关闭资源
	}
	
	//3.文件删除
	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException{

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");
			
		// 2 执行删除
		fs.delete(new Path("/hadoop-2.7.2.tar.gz"), true);
			
		// 3 关闭资源
		fs.close();
	}
	
	

	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "root");

		// 2 创建输入流
		FileInputStream fis = new FileInputStream(new File("D:/banhua.txt"));

		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));

		// 4 流对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	    fs.close();
	}
}
