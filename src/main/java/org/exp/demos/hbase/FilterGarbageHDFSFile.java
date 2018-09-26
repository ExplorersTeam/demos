package org.exp.demos.hbase;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterGarbageHDFSFile {
	public static final String JOBNAME="FilterJob";
	public static String HDFSFileName;
	public static List<String> SmallFiles ;

	public static long HDFSFileLength;
	
	public static Configuration conf;
	public static FileSystem fs;
	public static URI uri;
	public static Path path;
	public static FileStatus filestatus;
	
	/**
	 * 判别text对应的HDFS文件是否是垃圾文件
	 */
	public static boolean filter(String text, long Sum) throws IOException {
		try {
			uri = new URI(text);
			conf = new Configuration();
			fs = FileSystem.get(uri, conf);
			filestatus = fs.getFileStatus(path);
			HDFSFileLength = filestatus.getLen();
			if(((float)Sum/(float)HDFSFileLength)<0.2){
				return true;
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return false;
	}
	/**
	 * 
	 * @author lsg
	 * 对垃圾文件的操作
	 */
	
	static class HBaseFilterMapper extends TableMapper<Text,TextAndLongWritable>{
		public static enum Counters {ROW};

		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");
		byte[] qualifier_l = Bytes.toBytes("l");
		
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
			byte[] HDFSFileUri = columns.getColumnLatest(family, qualifier_n).getValue();
			byte[] SmallFileName = rowkey.get();
			byte[] SmallFileLength = columns.getColumnLatest(family, qualifier_l).getValue();
			String uri = HDFSFileUri.toString();
			String str = SmallFileName.toString();			
			long len=Bytes.toLong(SmallFileLength);
			TextAndLongWritable SmallFile = new TextAndLongWritable(str,len);
			context.write(new Text(uri), SmallFile);			
		}
	}
	
	static class FilterReducer extends Reducer<Text, TextAndLongWritable, Text, LongWritable>{
		
		public void reduce(Text text,Iterable<TextAndLongWritable> smallfiles,Context context) throws IOException, InterruptedException{
			long Sum = 0;
			for(TextAndLongWritable smallfile:smallfiles){
				Sum += smallfile.getLongWritable().get();
			}	
			if(filter(text.toString(),Sum)){
				context.write(text,new LongWritable(Sum));
			}
		}
	}
	
	
public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		String tablename = "dfs:dfs_file";
		String output = "GarbageHDFSFile";
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("s"));
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
		
		
		Job job = new Job(conf,JOBNAME);
		job.setJarByClass(FilterGarbageHDFSFile.class);
	
		/**
		 * initTableMapperJob在TableMap任务提交之前，对TableMap任务进行设置
		 */
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				HBaseFilterMapper.class,
				Text.class,
				TextAndLongWritable.class,
				job
				);
		job.setReducerClass(FilterReducer.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true)? 0:1);
	}


}
