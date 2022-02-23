package com.test.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;


public class HdfsOP {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master01:9000");

        FileSystem fileSystem = FileSystem.get(conf);
        FileInputStream fis = new FileInputStream("/Users/bruce/vscode/apache-maven-3.8.4/README.TXT");
        FSDataOutputStream fos = fileSystem.create(new Path("/README.TXT"));
        IOUtils.copyBytes(fis, fos, 1024, true);



    }
}

