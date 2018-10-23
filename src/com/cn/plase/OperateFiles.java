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
    //��ȡ�����ļ�
    static Configuration conf = new Configuration();
    static FileSystem hdfs;

    static {
    	//root�������ڵ�������û���
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("root"); 
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.default.name", "hdfs://10.28.150.93:9000/");
                    //conf.set("hadoop.job.ugi", "root");
                    //����������֧�� hdfs��׷�� ���ܵģ�hdfs.append()
                    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
                    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
                    Path path = new Path("hdfs://10.28.150.93:9000/");
                    //����ڱ��ز��ԣ���Ҫʹ�ô��ַ�����ȡ�ļ�ϵͳ
                    hdfs = FileSystem.get(path.toUri(), conf);
                    //hdfs = path.getFileSystem(conf); // ���Ҳ����
                    //�����Hadoop��Ⱥ�����У�ʹ�ô��ַ�������ֱ�ӻ�ȡĬ���ļ�ϵͳ
                    //hdfs = FileSystem.get(conf); //������У������õ���hdfs���в���������Ա����ļ�ϵͳ�����������hdfs�ģ�ԭ��̫���  
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

    //1.����hdfsĿ¼
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
    
    //2.�ļ�������
    @Test
    public void renameFile() throws IOException{
        String oldName = "/copy/a.txt";
        String newName = "/copy/atob.txt";
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        if (hdfs.exists(oldPath)){
            hdfs.rename(oldPath,newPath);
            System.out.println("rename�ɹ���");
        }else{
            System.out.println("�ļ�������!renameʧ��!");
        }
    }
    
    //3.��ȡ�ļ�
    @Test
    public void readFile() throws IOException{
        String uri = "/copy/atob.txt";
        //�ж��ļ��Ƿ����
        if(!hdfs.exists(new Path(uri))){
            System.out.println("Error ; the file not exists.");
            return;
        }
        InputStream in = null;
        try {
            in = hdfs.open(new Path(uri));
            //BufferedReader bf =new BufferedReader(new InputStreamReader(in,"GB2312"));//��ֹ��������
            //���Ƶ���׼�����
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
   
    //4.�½�һ���ļ�
    @Test
    public void createFile()
            throws IOException {
        String fileName = "/copy/test.txt";
        String fileContent = "";
        Path dst = new Path(fileName);
        //�ж� �½����ļ���hdfs���Ƿ����
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

    //5.׷�����ݵ��ļ�
    @Test
    public void appendFile()
            throws IOException {
        String fileName = "/copy/test.txt";
        String fileContent = "��� ����";
        Path dst = new Path(fileName);
        byte[] bytes = fileContent.getBytes();
        //����ļ�������
        if (!hdfs.exists(dst)) {
            System.out.println("Error : the file not exists");
            return;
        }
        FSDataOutputStream output = hdfs.append(dst);
        output.write(bytes);
        System.out.println("successful: append to file \t" + conf.get("fs.default.name")
                + fileName);
    }
    
    //6.�г������ļ�
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

    //7.�ж��ļ��Ƿ���ڣ����ڼ�ɾ��
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