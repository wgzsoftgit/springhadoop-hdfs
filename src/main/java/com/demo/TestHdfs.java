package com.demo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfs {
//HADOOP_USER_NAME   ==root
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.220.129:9000");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        System.out.println(conf);
        FileSystem fs = FileSystem.get(conf);
      
 
        //write
        Path path = new Path("/import/tmp/hello.txt");
        FSDataOutputStream fout = fs.create(path);
        byte[] bWrite = "hello hadoop distribute file system \n".getBytes();
        fout.write(bWrite); //写入字节数组
        fout.flush(); //flush提供了一种将缓冲区的数据强制刷新到文件系统的方法
        fout.close(); //关闭写出流

        fout = fs.append(path);
        fout.write("append: the append method of java API \n".getBytes());
        fout.close(); //关闭写出流

        //read 文件的具体内容
        FSDataInputStream fin = fs.open(path);
        byte[] buff = new byte[128];
        int len = 0 ;

        while( (len = fin.read(buff,0,128))  != -1 )
        {
            System.out.print(new String(buff,0,len));
        }

        //创建目录      
        if(fs.mkdirs(new Path("/import/test")))
        {
            System.out.println("mkdir /import/test success ");
        }

        //列出目录
        FileStatus[]  paths = fs.listStatus(new Path("/import"));
        for(int i = 0 ; i < paths.length ;++i)
        {//HdfsLocatedFileStatus{path=hdfs://192.168.220.129:9000/import/test; isDirectory=true; modification_time=1586445926242; access_time=0; owner=root; group=supergroup; permission=rwxr-xr-x; isSymlink=false; hasAcl=false; isEncrypted=false; isErasureCoded=false}
            System.out.println(paths[i].toString());
            System.out.println(paths[i].getLen());//0
            System.out.println(paths[i].isDirectory());//true
            System.out.println(paths[i].getPath().getParent());//hdfs://192.168.220.129:9000/import
            System.out.println(paths[i].getPath());//hdfs://192.168.220.129:9000/import/test
            System.out.println(paths[i].getPath().getName());//test
        }

        //删除
        if(fs.delete(new Path("/import"), true))
        {
            System.out.println("delete directory /import ");
        }

        fin.close();
        fs.close();
    }

}
//https://blog.csdn.net/cjf_wei/java/article/details/77140966