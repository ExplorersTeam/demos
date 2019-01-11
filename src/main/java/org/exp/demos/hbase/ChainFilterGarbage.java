package org.exp.demos.hbase;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ctg.ctdfs.core.common.DFSConstants;
import com.ctg.ctdfs.core.common.HBaseClient;
import com.google.common.collect.Maps;


public class ChainFilterGarbage {
	private static final Log LOG = LogFactory.getLog(ChainFilterGarbage.class);
	
	public static final String JOBNAME="FilterJob";
	public static final long SMALLFILELENGTH = 2097152;
	public static enum Counters {ROW,HDFSUriAmount,MigrationAmount,GarbageFileAmount,SmallFileAmount};

	
	public static long hdfsFileLength;
	
	public static Configuration conf;
	public static FileSystem fs;
	public static URI uri;
	public static Path path;
	public static FileStatus filestatus;
	
	private static HBaseClient hbaseClient;
	
	/**
	 * 判别text对应的HDFS文件是否是垃圾文件
	 */
	public static boolean filter(String text, long sum) throws IOException {
		try {
			uri = new URI(text);
			conf = new Configuration();
			fs = FileSystem.get(uri, conf);
			path=new Path(text);
			filestatus = fs.getFileStatus(path);
			hdfsFileLength = filestatus.getLen();
			float result = ((float)sum/(float)hdfsFileLength);
			if(result<0.2){
				return true;
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return false;
	}
	/**
	 * 判断记录是否是小文件
	 */
	public static boolean issmallfile(long length, String isdir){
		if(length<SMALLFILELENGTH && isdir.equals("false")){
			return true;
		}
		return false;
	}
	
	static class FilterMapper extends TableMapper<Text,Result>{
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");//the hdfs file uri
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		byte[] qualifier_d = Bytes.toBytes("d");//is dir or not of dfs file
		byte[] qualifier_i = Bytes.toBytes("i");//the startindex of dfs file in hdfs file

		@Override
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.ROW).increment(1);
			Cell hdfsFileUri = columns.getColumnLatestCell(family, qualifier_n);
			Cell fileLength = columns.getColumnLatestCell(family, qualifier_l);
			Cell isDir = columns.getColumnLatestCell(family, qualifier_d);
			if(hdfsFileUri!=null && fileLength!=null && isDir!=null ){
				String hdfsFileUriStr = Bytes.toString(CellUtil.cloneValue(hdfsFileUri));
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength))) ;
				String isdir = Bytes.toString(CellUtil.cloneValue(isDir));
				if(issmallfile(length,isdir)){
					context.getCounter(Counters.SmallFileAmount).increment(1);
					context.write(new Text(hdfsFileUriStr), columns);
				}
			}
		}
	}
	/**
	 * @author lsg
	 *	FilterReducer的输出<key,value>=<hdfsuri,List<Result>>
	 */
	static class FilterReducer extends Reducer<Text, Result, Text, List<Result>>{	
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		
		@Override
		public void reduce(Text text,Iterable<Result> smallfiles,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			long sum = 0;
			List<Result> dfsfiles = new ArrayList<Result>();
			for(Result smallfile:smallfiles){
				dfsfiles.add(smallfile);
				//......此处需要添加判断，将hdfs uri、startIndex、length相同的小文件剔除
				Cell fileLength = smallfile.getColumnLatestCell(family, qualifier_l);
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				sum += length;
			}	
			
			if(filter(text.toString(),sum) && !dfsfiles.isEmpty()){	
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text, dfsfiles);
			}
		}
	}
	
    public static Map<String, String> toMap(String status,String ctime,String datafile,
    		long offset,long length,String fileinfo,String isdir,String owner,
    		String group,String permission) {
        Map<String, String> dataMap = Maps.newHashMap();
        dataMap.put(DFSConstants.META_STATUS, status);
        dataMap.put(DFSConstants.META_CTIME, ctime);
        dataMap.put(DFSConstants.META_DATA_FILE, datafile);
        dataMap.put(DFSConstants.META_OFFSET, String.valueOf(offset));
        dataMap.put(DFSConstants.META_LENGTH, String.valueOf(length));
        dataMap.put(DFSConstants.META_FILE_INFO, fileinfo);
        dataMap.put(DFSConstants.META_IS_DIR, isdir);
        dataMap.put(DFSConstants.META_OWNER, owner);
        dataMap.put(DFSConstants.META_GROUP, group);
        dataMap.put(DFSConstants.META_PERMISSION, permission);
        return dataMap;
    }


	static class GcMapper extends Mapper<Text,List<Result>,Text,Text>{
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		byte[] qualifier_i = Bytes.toBytes("i");//the startindex of dfs file in hdfs file
		byte[] qualifier_s = Bytes.toBytes("s");
		byte[] qualifier_t = Bytes.toBytes("t");
		byte[] qualifier_f = Bytes.toBytes("f");
		byte[] qualifier_d = Bytes.toBytes("d");
		byte[] qualifier_o = Bytes.toBytes("o");
		byte[] qualifier_g = Bytes.toBytes("g");
		byte[] qualifier_p = Bytes.toBytes("p");
		
		String tableName = "dfs:dfs_file";
		Map<String,String> rowDataMap = Maps.newHashMap(); 
		Text success = new Text("SUCCESS");
		//map key=gabage hdfsuri value=dfsinfo
		@Override
		public void map(Text key,List<Result> smallfiles,Context context) throws IOException, InterruptedException{
			String datafile = key.toString();
			Path hdfsReadPath = new Path(datafile);
			Path hdfsWritePath = new Path(datafile +".temp");
			conf = new Configuration();
			fs = hdfsReadPath.getFileSystem(conf);
			FSDataInputStream in = null;
			FSDataOutputStream out = fs.create(hdfsWritePath,true);//temp文件覆盖写
			hbaseClient = new HBaseClient(conf);
			
			
			int offset = 0;
			//遍历dfs file，读取并写入新的hdfs file中
			for(Result smallfile:smallfiles){
				Cell i = smallfile.getColumnLatestCell(family, qualifier_i);
				Cell l = smallfile.getColumnLatestCell(family, qualifier_l);
				Cell s = smallfile.getColumnLatestCell(family, qualifier_s);
				Cell t = smallfile.getColumnLatestCell(family, qualifier_t);
				Cell f = smallfile.getColumnLatestCell(family, qualifier_f);
				Cell d = smallfile.getColumnLatestCell(family, qualifier_d);
				Cell o = smallfile.getColumnLatestCell(family, qualifier_o);
				Cell g = smallfile.getColumnLatestCell(family, qualifier_g);
				Cell p = smallfile.getColumnLatestCell(family, qualifier_p);
				
				String rowkey = Bytes.toString(smallfile.getRow());
				long startIndex = Bytes.toLong(CellUtil.cloneValue(i));
				int length =  Bytes.toInt(CellUtil.cloneValue(l));
				String status = Bytes.toString(CellUtil.cloneValue(s));
				String ctime = Bytes.toString(CellUtil.cloneValue(t));
				String fileinfo = Bytes.toString(CellUtil.cloneValue(f));
				String isdir = Bytes.toString(CellUtil.cloneValue(d));
				String owner = Bytes.toString(CellUtil.cloneValue(o));
				String group = Bytes.toString(CellUtil.cloneValue(g));
				String permission = Bytes.toString(CellUtil.cloneValue(p));
				
				byte[] buffer = new byte[length];
				in = fs.open(hdfsReadPath);
				int bytesReaded = in.read(startIndex, buffer, offset, length);
				while(bytesReaded != -1){
					in.read(startIndex, buffer, offset, length);
				}
				out.write(buffer, offset, length);
				startIndex = out.getPos();
				
				rowDataMap = toMap(status,ctime,datafile,startIndex,length,fileinfo,
						isdir,owner,group,permission);
				hbaseClient.put(tableName, rowkey, rowDataMap);
			}
			boolean flagDelete = fs.delete(hdfsReadPath, true);
			boolean flagRename = fs.rename(hdfsWritePath, hdfsReadPath);
			if(flagDelete&&flagRename){
				context.getCounter(Counters.MigrationAmount).increment(1);
				context.write(key, success);
			}
		}
	}

	
public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		String tablename = "dfs:dfs_file";
		String output = "GarbageHDFSFile";
		Scan scan = new Scan();
		/*
		 * 1、从HBase表中读入数据-TableInputFormat
		 * dfs:dfs_file表中记录了小文件的相关信息，通过该表可以获得小文件的大小以及所属HDFS文件
		 * f:n--->小文件所属HDFS文件uri，f:l--->小文件大小
		 * (map阶段)HDFS文件uri作为key，相应小文件列表作为value
		 * (reduce阶段)计算小文件容量之和
		 *
		 * 2、获取实际的HDFS文件信息
		 * 计算dfs:dfs_file表中出现的小文件在HDFS文件上的占比，如果占比小于20%，则该HDFS文件为垃圾文件
		 * 
		 * 3、判别出垃圾文件之后，进行垃圾回收
		 * (复制阶段)将垃圾文件中的小文件复制到一块新的文件上
		 * (删除阶段)删除垃圾文件
		 */
		Configuration conf = new Configuration();
		//每次运行程序之前删除运行结果的输出文件夹
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output),true);
		}
		fs.close();

		Job job = Job.getInstance(conf, JOBNAME);
		job.setJarByClass(ChainFilterGarbage.class);
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				FilterMapper.class,
				Text.class,
				Result.class,
				job
				);
		ChainReducer.setReducer(job, FilterReducer.class, Text.class, Result.class, Text.class, Result.class, conf);
		System.out.println("FilterReducer已完成......");
		ChainReducer.addMapper(job, GcMapper.class, Text.class, Result.class, Text.class, Text.class, conf);
		System.out.println("GcMapper已完成.......");
		
		job.setNumReduceTasks(1);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}
}
