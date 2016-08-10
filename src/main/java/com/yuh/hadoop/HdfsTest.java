package com.yuh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HdfsTest {

	public static void main(String[] args) throws Exception{
		HdfsTest test = new HdfsTest();
		test.getNodeList();
	}
	
	public void getNodeList() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for(int i=0; i<dataNodeStats.length; i++){
			System.out.println("DataNode_" + i + "_Name: " + dataNodeStats[i].getHostName());
		}
	}
	
	/**
	 * 查找某个文件在hdfs集群的位置
	 * @throws Exception
	 */
	public void fileLocation() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/abc.txt");
		FileStatus status = hdfs.getFileStatus(path);
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(status, 0, status.getLen());
		
		int blockLen = blkLocations.length;
		for(int i=0; i<blockLen; i++){
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_" + i + "_location: " + hosts[0]);
		}
	}
	
	public void listAllFiles() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/");
		FileStatus status[] = hdfs.listStatus(path);
		for(int i=0; i<status.length; i++){
			System.out.println(status[i].getPath().toString());
		}
	}
	
	public void getFileStatus() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/copy.txt");
		FileStatus status = hdfs.getFileStatus(path);
		System.out.println("modify time: " + status.getModificationTime());
	}
	
	public void checkFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/copy.txt");
		boolean result = hdfs.exists(path);
		System.out.println("result: " + result);
	}
	
	public void deleteDir() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/testdir");
		boolean result = hdfs.delete(path, true);
		//hdfs.delete(path, false)，后面的false表示不是递归删除的。
		//如果目录有文件，就会报错。
		//true，会删掉目录及其下的文件
		System.out.println("delete result: " + result);
	}
	
	public void deleteFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/testdir/zhihu.gif");
		boolean result = hdfs.delete(path, true);
		System.out.println("delete result: " + result);
	}
	
	public void renameFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path oldPath = new Path("/user/hadoop/input/abc2.txt");
		Path newPath = new Path("/user/hadoop/input/abc.txt");
		boolean result = hdfs.rename(oldPath, newPath);
		System.out.println("rename result: " + result);
	}
	
	public void createDir() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path("/user/hadoop/input/testdir");
//		hdfs.create(path);
		//create直接创建空文件。即使路径后面带了/，即/.../testdir/也是创建文件。
		hdfs.mkdirs(path);
	}
	
	public void createFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		byte[] buff = "This is a hadoop create file test.".getBytes();
		Path dfs = new Path("/user/hadoop/input/create");
		//注意，这个目录应该是最后创建文件的地址。
		//所以最好是在input目录下创建了一个create文件。
		
		FSDataOutputStream out = hdfs.create(dfs);
		out.write(buff);
		out.close();
	}
	
	/**
	 * 从本地复制文件到hdfs
	 * @throws Exception
	 */
	public void copyFile() throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		System.out.println(conf.get("fs.default.name"));

		// 本地文件
		Path src = new Path("D:\\zhihu.gif");

		// HDFS
		Path dst = new Path("/user/hadoop/input/");
		//这里直接写不对。需要将core-site.xml和hdfs-site.xml放入项目中

		hdfs.copyFromLocalFile(src, dst);
		System.out.println("Upload to" + conf.get("fs.default.name"));
		FileStatus files[] = hdfs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
	}

}
