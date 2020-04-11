package com.demo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.hdfs.DistributedFileSystem;



public class connectiondemo {
	public static void main(String[] args) throws IOException {
		String HDFS_PATH = "hdfs://192.168.220.129:9000"; //要连接的hadoop
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", HDFS_PATH);
		//连接文件系统,FileSystem用来与hadoop文件系统交互
		FileSystem fileSystem = FileSystem.get(configuration);
		 
		configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());  //设置处理分布式文件系统
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(configuration));
		//FsUrlStreamHandlerFactory()  不加参数连接会无法识别的hdfs协议,原因是hadoop在获取处理hdfs协议的控制器时获取了configuration的fs.hdfs.impl值
		 
		//获取Hadoop文件，并打印输出
		InputStream inputStream = new URL(HDFS_PATH + "/test/test.log").openStream();
		int size = inputStream.available();
		 
		for (int i = 0; i < size; i++){
		    System.out.print((char)inputStream.read());
		}
		inputStream.close();
	}
	
	//https://blog.csdn.net/a_lgorithm/java/article/details/95520248
}
