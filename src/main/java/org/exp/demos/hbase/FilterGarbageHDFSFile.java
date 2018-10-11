package org.exp.demos.hbase;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterGarbageHDFSFile {
	public static final String JOBNAME="FilterJob";
	public static String HDFSFileName;
	public static final long SMALLFILELENGTH = 2097152;
	public static List<String> SmallFiles ;
	public static enum Counters {ROW,HDFSUriAmount,GarbageFileAmount,SmallFileAmount};

	public static long HDFSFileLength;
	
	public static Configuration conf;
	public static FileSystem fs;
	public static URI uri;
	public static Path path;
	public static FileStatus filestatus;
	
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
			HDFSFileLength = filestatus.getLen();
			DecimalFormat df = new DecimalFormat("0.000");
			double result = Double.valueOf(df.format((float)sum/(float)HDFSFileLength));
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
	/**
	 * 
	 * @author lsg
	 * f:n->数据形式:hdfs://ctdfs/apps/dfs/test/_dfs.10.142.90.152.23731.1.20180115172231083
	 * 需要解析出文件名
	 */
//	public static String GabageFile(){
//		
//	}
	/**
	 * @author lsg
	 * 垃圾回收操作：主要对HDFS文件操作
	 * 1、(复制阶段)将垃圾文件中的小文件复制到一块新的文件上
	 * 2、(删除阶段)删除垃圾文件
	 */
//	public static boolean gc(String HDFSFileName){
//		
//		return 
//	}
	
	static class FilterMapper extends TableMapper<Text,TextAndLongWritable>{
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");
		byte[] qualifier_l = Bytes.toBytes("l");
		byte[] qualifier_d = Bytes.toBytes("d");

		/*
		 * map()获取HBase表中的小文件数据
		 */
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.ROW).increment(1);
			/*
			 * 1、获取HDFSFileUri、SmallFileName、SmallFileLength
			 * 2、以HDFSFileUri为key，SmallFileName构成的ArrayList为value
			 * 
			 */
			Cell HDFSFileUri = columns.getColumnLatestCell(family, qualifier_n);
			byte [] SmallFileName = columns.getRow();
			Cell FileLength = columns.getColumnLatestCell(family, qualifier_l);
			Cell IsDir = columns.getColumnLatestCell(family, qualifier_d);
			if(HDFSFileUri!=null && FileLength!=null && IsDir!=null ){
				String HDFSFileUri_str = Bytes.toString(CellUtil.cloneValue(HDFSFileUri));
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(FileLength))) ;
				String isdir = Bytes.toString(CellUtil.cloneValue(IsDir));
				if(issmallfile(length,isdir)){
					context.getCounter(Counters.SmallFileAmount).increment(1);
					String SmallFileName_str = Bytes.toString(SmallFileName);
					TextAndLongWritable SmallFile = new TextAndLongWritable(SmallFileName_str,length);
					context.write(new Text(HDFSFileUri_str), SmallFile);
				}
	
			}		
		}
	}
//	static class GCMapper extends Mapper <LongWritable, Text, Text, LongWritable>{
//		public void map(LongWritable key, Text value, Context context){
//			
//		}
//	}
	static class FilterReducer extends Reducer<Text, TextAndLongWritable, Text, Text>{
		
		public void reduce(Text text,Iterable<TextAndLongWritable> smallfiles,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			long sum = 0;
			StringBuilder sb = new StringBuilder();
			for(TextAndLongWritable smallfile:smallfiles){
				sum += smallfile.getLongWritable().get();
				sb.append(smallfile.toString());
			}	
			if(filter(text.toString(),sum)){
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text,new Text(sb.toString()));
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
		
		Job job = new Job(conf,JOBNAME);
		job.setJarByClass(FilterGarbageHDFSFile.class);
	
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				FilterMapper.class,
				Text.class,
				TextAndLongWritable.class,
				job
				);
		job.setReducerClass(FilterReducer.class);
		
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true)? 0:1);
	}


}
