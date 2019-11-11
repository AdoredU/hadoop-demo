package com.adoredu.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.net.URI;

public class HDFSTestClient {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node-1:9000");
        // 设置客户的身份
//        System.setProperty("HADOOP_USER_NAME", "gp");
        FileSystem fs = FileSystem.get(conf);
        // 也可以通过如下方式指定文件系统类型并且设置用户身份
        // FileSystem fs = FileSystem.get(new URI("hdfs://node-1:9000"), conf, "gp");
        // 创建一个目录
//        fs.create(new Path("/helloByJava"));
        // 下载文件
//        fs.copyToLocalFile(new Path("/tmp/input/wordcount.txt"), new Path("/Users/gp/Desktop/tmp"));
        // 下载后会在对应路径下有wordcount.txt和一个标识成功的.wordcount.txt.crc文件

        // 流式操作，更底层的操作
        // 将本地文件上传到hdfs
        FSDataOutputStream outputStream = fs.create(new Path("/test.txt"), true);
        FileInputStream inputStream = new FileInputStream("/Users/gp/Desktop/tmp/test.txt");
        IOUtils.copy(inputStream, outputStream);

        // 关闭连接
        fs.close();
    }
}
