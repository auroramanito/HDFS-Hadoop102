package com.jinse.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {
	 public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
	        //1.获取hdfs客户端对象

	        Configuration conf=new Configuration();
	        conf.set("fs.defaultFS","hdfs://hadoop102:9000");
	      //  FileSystem fs=FileSystem.get(conf);
	        FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
	        //2.在hdfs上创建路径
	        fs.mkdirs(new Path("/hadoop102/jinse"));

	        //3.关闭资源
	        fs.close();
	        System.out.println("over");
	    }
}
