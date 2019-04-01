package org.exp.demos.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseOperationTest2 {
	private static final Log LOG = LogFactory.getLog(HBaseOperationTest2.class);

	public HBaseOperationTest2() {

	}

	static class GCMapper extends Mapper<LongWritable, Text, Text, Text> {
		String tableName = "dfs:dfs_file";
		Table table = null;
		Connection connection = null;
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			connection = ConnectionFactory.createConnection(configuration);
			LOG.info("connection has been established");
			table = connection.getTable(TableName.valueOf(tableName));
			LOG.info("table name is : " + table.getName().getNameAsString());
		};

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String datafile = value.toString().trim();
			LOG.info("datafile is : " + datafile);
			Scan scan = new Scan();
			SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier_n, CompareOp.EQUAL, datafile.getBytes());
			scan.setFilter(singleColumnValueFilter);
			LOG.info("is filter set successfully? " + scan.hasFilter());
			ResultScanner results = table.getScanner(scan);
			LOG.info("Got result? [" + results.iterator().hasNext() + "].");
			for (Result result : results) {
				LOG.info("Got a result, content is [" + result.toString() + "].");
				context.write(value, new Text(result.toString()));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			table.close();
			connection.close();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputPath = new Path("hdfs:///user/dfs/GarbageHDFSFile");
		Path outputPath = new Path("hdfs:///user/dfs/HBaseOperationTest2OutPut");
		Configuration conf = new Configuration();
		// 通过输入流读取配置文件
		InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
		InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
		InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(coreSiteInputStream);
		conf.addResource(hdfsSiteInputStream);
		conf.addResource(hbaseSiteInputStream);

		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}

		Job job = new Job(conf, "HBaseOperationTest2");
		job.setJarByClass(HBaseOperationTest2.class);
		job.setMapperClass(GCMapper.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
