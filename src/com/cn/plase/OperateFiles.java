package com.cn.plase;
import java.io.*;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;


public class OperateFiles {
    // initialization
    //读取配置文件
    static Configuration conf = new Configuration();
    static FileSystem hdfs;

    static {
    	//root是你主节点虚机的用户名
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("root"); 
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.default.name", "hdfs://10.28.150.93:9000/");
                    //conf.set("hadoop.job.ugi", "root");
                    //以下两行是支持 hdfs的追加 功能的：hdfs.append()
                    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
                    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
                    Path path = new Path("hdfs://10.28.150.93:9000/");
                    //如果在本地测试，需要使用此种方法获取文件系统
                    hdfs = FileSystem.get(path.toUri(), conf);
                    //hdfs = path.getFileSystem(conf); // 这个也可以
                    //如果在Hadoop集群下运行，使用此种方法可以直接获取默认文件系统
                    //hdfs = FileSystem.get(conf); //这个不行，这样得到的hdfs所有操作都是针对本地文件系统，而不是针对hdfs的，原因不太清楚  
                    return null;
                }
            });
        } catch (IOException e) {
            // TODO Auto-generated catch block  
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block  
            e.printStackTrace();
        }
    }

    //1.创建hdfs目录
    @Test
    public void createDir() throws IOException {
        String dir = "/AddTestDir/";
        Path path = new Path(dir);
        if (hdfs.exists(path)) {
            System.out.println("dir \t" + conf.get("fs.default.name") + dir
                    + "\t already exists");
            return;
        }
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }
    
    //2.文件重命名
    @Test
    public void renameFile() throws IOException{
        String oldName = "/copy/a.txt";
        String newName = "/copy/atob.txt";
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        if (hdfs.exists(oldPath)){
            hdfs.rename(oldPath,newPath);
            System.out.println("rename成功！");
        }else{
            System.out.println("文件不存在!rename失败!");
        }
    }
    
    //3.读取文件
    @Test
    public void readFile() throws IOException{
        String uri = "/copy/atob.txt";
        //判断文件是否存在
        if(!hdfs.exists(new Path(uri))){
            System.out.println("Error ; the file not exists.");
            return;
        }
        InputStream in = null;
        try {
            in = hdfs.open(new Path(uri));
            //BufferedReader bf =new BufferedReader(new InputStreamReader(in,"GB2312"));//防止中文乱码
            //复制到标准输出流
            IOUtils.copyBytes(in, System.out, 4096,false);
            /*String line = null;
            while((line = bf.readLine()) != null){
                System.out.println(line);
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }
    }
   
    //4.新建一个文件
    @Test
    public void createFile()
            throws IOException {
        String fileName = "/copy/test.txt";
        String fileContent = "";
        Path dst = new Path(fileName);
        //判断 新建的文件在hdfs上是否存在
        if(hdfs.exists(dst)){
            System.out.println("Error : the hdfs file exists.");
        }else {
            byte[] bytes = fileContent.getBytes();
            FSDataOutputStream output = hdfs.create(dst);
            output.write(bytes);
            System.out.println("new file \t" + conf.get("fs.default.name")
                    + fileName);
        }
    }

    //5.追加内容到文件
    @Test
    public void appendFile()
            throws IOException {
        String fileName = "/copy/test.txt";
        String fileContent = "你好 世界";
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        //如果文件不存在
        if (!hdfs.exists(dst)) {
            System.out.println("Error : the file not exists");
            return;
        }
        FSDataOutputStream output = hdfs.append(dst);
        output.write(bytes);
        System.out.println("successful: append to file \t" + conf.get("fs.default.name")
                + fileName);
    }
    
    //6.列出所有文件
    @Test
    public void listFiles() throws IOException {
        String dirName = "/copy/";
        Path f = new Path(dirName);
        FileStatus[] status = hdfs.listStatus(f);
        System.out.println(dirName + " has all files:");
        if (status.length == 0) {
            System.out.println("nothing !");
        } else {
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath().toString());
            }
        }
    }

    //7.判断文件是否存在，存在即删除
    @Test
    public void deleteFile() throws IOException {
        String fileName = "/copy/atob.txt";
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) { // if exists, delete  
            boolean isDel = hdfs.delete(f, true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + "notExists");
        }
    }
}