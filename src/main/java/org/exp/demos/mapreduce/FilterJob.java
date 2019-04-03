package org.exp.demos.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.exp.demos.writable.PartMetadataWritable;

public class FilterJob {
	private static final Log LOG = LogFactory.getLog(FilterJob.class);
	private static final String JOBNAME = "FilterJob";
	public static final long SMALLFILELENGTH = 2097152;
	private static final double SMALLFILE = 0.2;

	private Configuration configuration;
	private static FileSystem fileSystem;
	private static byte[] family = Bytes.toBytes("f");
	private static byte[] qualifier_l = Bytes.toBytes("l");// the length of dfs file
	private static byte[] qualifier_i = Bytes.toBytes("i");// the startindex of dfs file in hdfs file
	private static byte[] qualifier_n = Bytes.toBytes("n");// hdfs file uri
	private static byte[] qualifier_d = Bytes.toBytes("d");// is dir or file

	public static enum Counters {
		ROW, HDFSUriAmount, MigrationAmount, GarbageFileAmount, SmallFileAmount
	};

	public FilterJob() throws FileNotFoundException {
		configuration = new Configuration();
		// 通过输入流读取配置文件
		InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
		InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
		InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
		InputStream yarnSiteInputStream = new FileInputStream(new File("/etc/hadoop/conf/yarn-site.xml"));
		configuration.addResource(coreSiteInputStream);
		configuration.addResource(hdfsSiteInputStream);
		configuration.addResource(hbaseSiteInputStream);
		configuration.addResource(yarnSiteInputStream);
		configuration.set("mapreduce.job.user.classpath.first", "true");
		configuration.set("mapreduce.task.classpath.user.precedence", "true");
		configuration.set("mapred.textoutputformat.separator", " ");
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * 判别text对应的HDFS文件是否是垃圾文件
	 */
	public static boolean filter(FileSystem fileSystem, String text, long sum) throws IOException {
		Path path = new Path(text);
		if (!fileSystem.exists(path)) {
			return false;
		}
		FileStatus filestatus = fileSystem.getFileStatus(path);
		long hdfsFileLength = filestatus.getLen();
		if (hdfsFileLength == 0) {
			return false;
		}
		DecimalFormat df = new DecimalFormat("0.000");
		LOG.info("HDFSFile : " + text + ", sum is : " + sum + ", HDFSFileLength is : " + hdfsFileLength);
		double result = Double.valueOf(df.format(sum / (float) hdfsFileLength));
		if (result < SMALLFILE) {
			return true;
		}
		return false;
	}

	/**
	 * 判断记录是否是小文件
	 */
	public static boolean issmallfile(long length, String isdir) {
		if (length < SMALLFILELENGTH && "false".equals(isdir)) {
			return true;
		}
		return false;
	}

	static class FilterMapper extends TableMapper<Text, Result> {
		@Override
		public void map(ImmutableBytesWritable rowkey, Result columns, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.ROW).increment(1);
			Cell hdfsFileUri = columns.getColumnLatestCell(family, qualifier_n);
			Cell fileLength = columns.getColumnLatestCell(family, qualifier_l);
			Cell isDir = columns.getColumnLatestCell(family, qualifier_d);
			if (hdfsFileUri != null && fileLength != null && isDir != null) {
				String hdfsFileUriStr = Bytes.toString(CellUtil.cloneValue(hdfsFileUri));
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				String isdir = Bytes.toString(CellUtil.cloneValue(isDir));
				if (issmallfile(length, isdir)) {
					context.getCounter(Counters.SmallFileAmount).increment(1);
				}
				context.write(new Text(hdfsFileUriStr), columns);
			}
		}
	}

	static class FilterReducer extends Reducer<Text, Result, Text, Text> {
		@Override
		public void setup(Context context) throws IOException {
			fileSystem = FileSystem.get(context.getConfiguration());
		}

		@Override
		public void reduce(Text text, Iterable<Result> smallfiles, Context context) throws IOException, InterruptedException {
			// 利用TreeSet过滤重复小文件
			TreeSet<PartMetadataWritable> treeSet = new TreeSet<>();
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			for (Result smallfile : smallfiles) {
				Cell fileLength = smallfile.getColumnLatestCell(family, qualifier_l);
				Cell fileStart = smallfile.getColumnLatestCell(family, qualifier_i);
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				long start = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileStart)));
				String rowkey = Bytes.toString(smallfile.getRow());
				PartMetadataWritable partMetadataWritable = new PartMetadataWritable(rowkey, length, start, text.toString());
				treeSet.add(partMetadataWritable);
			}
			long sum = 0;
			StringBuilder sb = new StringBuilder();
			for (PartMetadataWritable partMetadataWritable : treeSet) {
				long length = partMetadataWritable.getLength().get();
				sum += length;
				sb.append(partMetadataWritable.toString());
			}
			if (filter(fileSystem, text.toString(), sum)) {
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text, new Text(sb.toString()));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {
			fileSystem.close();
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String tablename = args[0];
		String output = args[1];
		Scan scan = new Scan();
		FilterJob filterJob = new FilterJob();
		Configuration conf = filterJob.getConfiguration();
		// 每次运行程序之前删除运行结果的输出文件夹
		fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(output))) {
			fileSystem.delete(new Path(output), true);
		}
		Job job = new Job(conf, JOBNAME);
		job.setJarByClass(FilterJob.class);
		TableMapReduceUtil.initTableMapperJob(tablename, scan, FilterMapper.class, Text.class, Result.class, job);
		job.setReducerClass(FilterReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		fileSystem.close();
	}
}
