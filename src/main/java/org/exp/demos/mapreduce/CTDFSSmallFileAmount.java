package org.exp.demos.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author lsg
 *
 */
public class CTDFSSmallFileAmount{
	public static final String JOBNAME="SmallFileCounter";
	public static final int SMALLFILELENGTH = 2097152;
	
	static class CountTableMapper extends TableMapper<Text,LongWritable>{
		/*
		 * map()方法处理RecordReader中读到的数据
		 * Mapper将原始数据转化成更有用的数据类型
		 * HBase提供的TableMapper类，将键强制转换成ImmutableBytesWritable类型，将值强制转换成Result类型
		 * 
		 */
		public enum Counters{ROWS,SMALLFILEAMOUNT};
		
		/*
		 * 判断是文件还是目录，如果是文件判断文件大小
		 */
		private boolean isSmallFile(long length,boolean isdictionary){
			if(length<=SMALLFILELENGTH&&(!isdictionary)){
				return true;
			}
			return false;
		}
		byte[] family = Bytes.toBytes("f");
		byte[] qulifier_l = Bytes.toBytes("l");
		byte[] qulifier_d = Bytes.toBytes("d");
		@Override
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException{
			/*
			 * 1、行数
			 * 2、符合小文件标准(大小小于等于2MB)的文件数量(2MB=2*1024*1024Byte 即2097152个字节)
			 */
			byte[] a=columns.getColumnLatest(family, qulifier_d).getValue();
			boolean isdictionary=Bytes.toBoolean(a);
			byte[] b = columns.getColumnLatest(family, qulifier_l).getValue();
			long length = Bytes.toLong(b);
			context.getCounter(Counters.ROWS).increment(1);
			if(isSmallFile(length,isdictionary)){
				context.getCounter(Counters.SMALLFILEAMOUNT).increment(1);
			}
		}
	}
	static class CountTableReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int count=0;
			for(LongWritable one:values){
				count++;
			}
			context.write(key, new LongWritable(count));
		}
	}
	public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		
		String tablename = "dfs:dfs_file";
		String output = "SmallFileAmount";
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("f"));
		/*
		 * 1、从HBase表中读入数据-TableInputFormat
		 */
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf,JOBNAME);
		job.setJarByClass(CTDFSSmallFileAmount.class);
		

		/*
		 * TableMapReduceUtil初始化job的参数设置
		 * scan：扫描器
		 * Mapper
		 * 1、使用TableMapReduceUtil来设置表的map阶段
		 * 2、使用setReducerClass来设置表的reduce阶段
		 */
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				CountTableMapper.class,
				Text.class,
				LongWritable.class,
				job
				);
		job.setReducerClass(CountTableReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true)? 0:1);
	}
	
}
