package org.exp.demos.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.xml.serialize.OutputFormat;


/**
 * @author lsg
 *
 */
public class CTDFSSmallFileAmount{
	public static final String JOBNAME="SmallFileCounter";
	public static final double SMALLFILELENGTH = 2097152;
	public enum Counters{ROWS,SMALLFILEAMOUNT};
	
	static class CountTableMapper extends TableMapper<Text,LongWritable>{
		/*
		 * 判断是文件还是目录，如果是文件判断文件大小
		 */
		private boolean isSmallFile(double length,String isdictionary){
			if(length<=SMALLFILELENGTH&&(isdictionary.equals("false"))){
				return true;
			}
			return false;
		}
		byte[] family = Bytes.toBytes("f");
		byte[] qulifier_l = Bytes.toBytes("l");
		byte[] qulifier_d = Bytes.toBytes("d");
		@Override
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException, InterruptedException{
			/**
			 * 1、行数
			 * 2、符合小文件标准(大小小于等于2MB)的文件数量(2MB=2*1024*1024Byte 即2097152个字节)
			 * Result: Single row result of a {@link Get} or {@link Scan} query.<p>
			 * 
			 **/
			Cell a = columns.getColumnLatestCell(family, qulifier_d);
			Cell b = columns.getColumnLatestCell(family, qulifier_l);
			if(a!=null && b!=null){
				String isdictionary = Bytes.toString(CellUtil.cloneValue(a));
				String filelength = Bytes.toString(CellUtil.cloneValue(b));
				context.getCounter(Counters.ROWS).increment(1);
				double Filelength =  Double.valueOf(filelength);
				if(isSmallFile(Filelength,isdictionary)){
					context.getCounter(Counters.SMALLFILEAMOUNT).increment(1);
				}
			}
		}
	}

	public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		
		String tablename = "dfs:dfs_file";
		String output = "SmallFileAmount";
		/**
		 * 1、创建扫描器
		 */
		Scan scan = new Scan();
		System.out.println("扫描器创建成功......");
		/**
		 * 2、读取配置
		 */
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.set("hbase.zookeeper.quorum", "dfs1a1.ecld.com,dfs1m1.ecld.com,dfs1m2.ecld.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase-secure");
		conf.set("hbase.rootdir", "hdfs://ctdfs/apps/hbase/data");
		conf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
		/**
		 * 3、配置作业
		 */
		Job job = new Job(conf,JOBNAME);
		job.setJarByClass(CTDFSSmallFileAmount.class);
		System.out.println("作业配置成功......");

		/*
		 * TableMapReduceUtil初始化job的参数设置
		 * 1、使用TableMapReduceUtil来设置表的map阶段
		 * 2、使用setReducerClass来设置表的reduce阶段
		 * 
		 * waitForCompletion(boolean b) 将job提交到集群，等待job执行完成
		 * b(true or false) 是否向用户打印详细的进度
		 */
		System.out.println("开始配置map、reduce任务");
		/**
		 * job 将输入数据集切分成若干独立的数据块，并分布到不同的节点
		 * map 多个map任务并行处理
		 */
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				CountTableMapper.class,
				ImmutableBytesWritable.class,
				Result.class,
				job
				);
		System.out.println("map任务配置成功");
		System.out.println("设置Reducer数量：0");
		job.setNumReduceTasks(0);
		System.out.println("设置计算结果的输出路径："+output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.out.println("开始执行任务："+JOBNAME);
		System.exit(job.waitForCompletion(true)? 0:1);
	}
	
}
