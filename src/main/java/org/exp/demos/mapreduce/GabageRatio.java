package org.exp.demos.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.exp.demos.writable.PartMetadataWritable;

public class GabageRatio {
	private static final Log LOG = LogFactory.getLog(GabageRatio.class);
	public static final String JOBNAME="GabageRatio";
	public enum Counters {
		ROWS, ROWWithHDFSURI, DIFFERENTHDFSURI, EXISTINGHDFSFILE
	};

	private Configuration configuration;
	private static byte[] family = Bytes.toBytes("f");
	private static byte[] qualifier_l = Bytes.toBytes("l");// the length of dfs file
	private static byte[] qualifier_i = Bytes.toBytes("i");// the startindex of dfs file in hdfs file
	private static byte[] qualifier_n = Bytes.toBytes("n");// hdfs file uri
	private static byte[] qualifier_s = Bytes.toBytes("s");
	private static byte[] qualifier_t = Bytes.toBytes("t");
	private static byte[] qualifier_f = Bytes.toBytes("f");
	private static byte[] qualifier_d = Bytes.toBytes("d");// is dir or file
	private static byte[] qualifier_o = Bytes.toBytes("o");
	private static byte[] qualifier_g = Bytes.toBytes("g");
	private static byte[] qualifier_p = Bytes.toBytes("p");
	private static FileSystem fileSystem;

	public GabageRatio() throws FileNotFoundException {
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
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	static class GabageRatioMapper extends TableMapper<Text, Result> {
		@Override
		public void setup(Context context) {
			LOG.info("&&&&&&&&&&&" + context.getConfiguration().get("hbase.regionserver.kerberos.principal"));
		}
		
		@Override 
		public void map(ImmutableBytesWritable rowkey, Result columns, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.ROWS).increment(1);
			LOG.info("-----" + context.getConfiguration().get("hbase.regionserver.kerberos.principal"));
			Cell nCell = columns.getColumnLatestCell(family, qualifier_n);
			Cell dCell = columns.getColumnLatestCell(family, qualifier_d);
			LOG.info("=======nCell:" + nCell + ", dCell:" + dCell);
			if (dCell != null && nCell != null) {
				String nStr = Bytes.toString(CellUtil.cloneValue(nCell));
				String dStr = Bytes.toString(CellUtil.cloneValue(dCell));
				LOG.info("-----nStr:" + nStr + ", dStr:" + dStr);
				if ("false".equals(dStr) && StringUtils.isNotBlank(nStr)) {
					context.write(new Text(nStr), columns);
					context.getCounter(Counters.ROWWithHDFSURI).increment(1);
				}
			}
		}
	}

	static class GabageRatioReducer extends Reducer<Text, Result, Text, Text> {
		DecimalFormat df = null;
		String gabageRatioStr = "";

		@Override
		public void setup(Context context) throws IOException {
			fileSystem = FileSystem.get(context.getConfiguration());
			df = new DecimalFormat("0.000");
		}

		@Override
		public void reduce(Text hdfsuri, Iterable<Result> results, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.DIFFERENTHDFSURI).increment(1);
			TreeSet<PartMetadataWritable> treeSet = new TreeSet<>();
			for (Result result : results) {
				Cell fileLength = result.getColumnLatestCell(family, qualifier_l);
				Cell fileStart = result.getColumnLatestCell(family, qualifier_i);
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				long start = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileStart)));
				String rowkey = Bytes.toString(result.getRow());
				PartMetadataWritable partMetadataWritable = new PartMetadataWritable(rowkey, length, start, hdfsuri.toString());
				treeSet.add(partMetadataWritable);
			}
			long sum = 0;
			for (PartMetadataWritable partMetadataWritable : treeSet) {
				sum += partMetadataWritable.getLength().get();
			}
			Path path = new Path(hdfsuri.toString());
			try {
				if (fileSystem.exists(path)) {
					context.getCounter(Counters.EXISTINGHDFSFILE).increment(1);
					long hdfsFileLength = fileSystem.getFileStatus(path).getLen();
					if (hdfsFileLength > 0) {
						long gabage = hdfsFileLength - sum;
						double gabageRatio = Double.valueOf(df.format(gabage / (float) hdfsFileLength));
						gabageRatioStr = String.valueOf(gabageRatio);
					} else {
						gabageRatioStr = "hdfs文件长度等于0,无法计算垃圾占比";
					}
				}
			} catch (IOException exception) {
				LOG.error(path + " is not exist!");
			}
			context.write(hdfsuri, new Text(gabageRatioStr));
		}

		@Override
		public void cleanup(Context context) throws IOException {
			fileSystem.close();
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Scan scan = new Scan();
		String tablename = args[0];
		String output = args[1];
		GabageRatio gabageRatio = new GabageRatio();
		Configuration configuration = gabageRatio.getConfiguration();
		System.out.println("+++++" + configuration.get("hbase.regionserver.kerberos.principal"));
		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job = new Job(configuration, JOBNAME);
		job.setJarByClass(GabageRatio.class);
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				GabageRatioMapper.class,
				Text.class,
				Result.class,
				job
		);
		job.setReducerClass(GabageRatioReducer.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
