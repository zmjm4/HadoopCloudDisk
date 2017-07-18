package com.model;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class HdfsDAO {

    //HDFS访问地址
    private static final String HDFS = "hdfs://192.168.1.104:9000";
 
 
    
    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    //hdfs路径
    private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf;

     
    
    //启动函数
    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.mkdirs("/Tom");
        //hdfs.copyFile("C:\\files", "/wgc/");
        //hdfs.ls("hdfs://192.168.1.104:9000/wgc/files");
        //hdfs.rmr("/wgc/files");
        //hdfs.download("/wgc/（3）windows下hadoop+eclipse环境搭建.docx", "c:\\");
        //System.out.println("success!");
    }        
    
    //加载Hadoop配置文件
    public  static JobConf config(){
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    //在根目录下创建文件夹
    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            //System.out.println("Create: " + folder);
        }
        fs.close();
    }
    
    //某个文件夹的文件列表
    public FileStatus[] ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        //System.out.println("ls: " + folder);
        //System.out.println("==========================================================");
        if(list != null)
        for (FileStatus f : list) {
            //System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        	System.out.printf("%s, folder: %s, 大小: %dK\n", f.getPath().getName(), (f.isDir()?"目录":"文件"), f.getLen()/1024);
        }
      //  System.out.println("==========================================================");
        fs.close();
        
        return  list;
    }
    
    
    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        //remote---/用户/用户下的文件或文件夹
        fs.copyFromLocalFile(new Path(local), new Path(remote));
         //System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }
    
    //删除文件或文件夹
    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        //System.out.println("Delete: " + folder);
        fs.close();
    }
    
    
    //下载文件到本地系统
    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }
    
    
}