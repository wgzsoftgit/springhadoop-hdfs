package com.demo;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Get_HDFS_Files {

    public static void hdfsFileTest() throws IOException {
        String uri = "hdfs://192.168.220.129:9000/";
        org.apache.hadoop.conf.Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);

        // 列出hdfs根目录下所有文件和目录（仅根目录一层）
        FileStatus[] status = fs.listStatus(new Path("/"));
        for (FileStatus sta : status) {
            System.out.println(sta);
        }

        // 在 hdfs 中新建文件并写入中英文数据
        FSDataOutputStream os = fs.create((new Path("/test/test.log")));
        os.write("hello hadoop ! 非常棒。".getBytes());
        os.flush();
        os.close();

        // 读取文件数据并打印至控制台显示
        InputStream is = fs.open(new Path("/test/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
       
       
        
        
        
    }

    public static void main(String[] args) throws IOException {
        hdfsFileTest();
    }
}
//https://blog.csdn.net/qq_24452475/java/article/details/79874630