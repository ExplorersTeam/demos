package org.exp.demos.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CollectJob {
	private static final Log LOG = LogFactory.getLog(CollectJob.class);
	private static final String JOBNAME = "CollectJob";
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
	private Configuration configuration;

	public static enum Counters {
		ROW, HDFSUriAmount, MigrationAmount, GarbageFileAmount, SmallFileAmount
	};

	public CollectJob() throws FileNotFoundException {
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

	/**
	 * 从A文件读出指定数据，写入B文件
	 */
	public static boolean migrationData(FSDataInputStream inputStream, FSDataOutputStream outputStream, Long position, int length) {
		byte[] buffer = new byte[length];
		int offset = 0;
		try {
			int readBytes = inputStream.read(position, buffer, offset, length);
			outputStream.write(buffer);
			LOG.info("the bytes has been readed is : " + readBytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	private static String getValue(Result result, byte[] family, byte[] qualifier) {
		Cell cell = result.getColumnLatestCell(family, qualifier);
		String value = Bytes.toString(CellUtil.cloneValue(cell));
		return value;
	}

	static class GcMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private Configuration configuration = null;
		Connection connection = null;
		Table table = null;
		// map key=gabage hdfsuri value=dfsinfo
		@Override
		public void setup(Context context) throws IOException {
			// System.setProperty("sun.security.krb5.debug", "true");
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
			LOG.info("sun.security.krb5.debug:" + System.getProperty("sun.security.krb5.debug"));
			LOG.info("javax.security.auth.useSubjectCredsOnly:" + System.getProperty("javax.security.auth.useSubjectCredsOnly"));
			configuration = context.getConfiguration();
			LOG.info("-----hbase.regionserver.kerberos.principal:" + configuration.get("hbase.regionserver.kerberos.principal"));
			LOG.info("-----hbase.master.kerberos.principal:" + configuration.get("hbase.master.kerberos.principal"));
			connection = ConnectionFactory.createConnection(configuration);
			LOG.info("-----tablename:" + configuration.get("tablename"));
			table = connection.getTable(TableName.valueOf(configuration.get("tablename")));
			LOG.info("Table name is [" + table.getName().getNameAsString() + "].");
			fileSystem = FileSystem.get(configuration);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] info = value.toString().split(" ");
			String datafile = info[0];
			String[] smallfiles = info[1].split(";");

			// 文件元数据修改过程(操作hbase dfs:dfs_file表)
			LOG.info("datafile uri is : " + datafile);
			Path hdfsReadPath = new Path(datafile);
			// 操作HDFS文件
			if (fileSystem.exists(new Path(datafile + ".temp"))) {
				fileSystem.delete(new Path(datafile + ".temp"), true);
			}
			Path hdfsWritePath = new Path(datafile + ".temp");
			// 通过job的context获取Configuration对象
			LOG.info("ZooKeeper quorum is [" + configuration.get("hbase.zookeeper.quorum") + "].");
			FSDataInputStream in = fileSystem.open(hdfsReadPath);
			FSDataOutputStream out = fileSystem.create(hdfsWritePath, true);// temp文件覆盖写
			// 文件拷贝过程
			int start = 0;
			List<Put> puts = new ArrayList<>();
			for (String smallfile : smallfiles) {
				String[] partSmallfile = smallfile.split(",");
				LOG.info("rowkey is : " + partSmallfile[0] + ", length is : " + partSmallfile[1] + ", start is : " + partSmallfile[2]);
				byte[] rowkey = Bytes.toBytes(partSmallfile[0]);
				Get get = new Get(rowkey);
				Result result = table.get(get);
				LOG.info("result is : " + result.toString());
				int length = Integer.valueOf(partSmallfile[1]);
				long position = Long.valueOf(partSmallfile[2]);
				boolean flag = migrationData(in, out, position, length);
				LOG.info("is migration suceess or not ? " + flag);
				// 修改元数据
				if (flag) {
					Put put = new Put(rowkey);
					put.addColumn(family, qualifier_i, result.getColumnLatestCell(family, qualifier_i).getTimestamp(), Bytes.toBytes(String.valueOf(start)));
					put.addColumn(family, qualifier_l, result.getColumnLatestCell(family, qualifier_l).getTimestamp(), Bytes.toBytes(String.valueOf(length)));
					put.addColumn(family, qualifier_d, result.getColumnLatestCell(family, qualifier_d).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_d)));
					put.addColumn(family, qualifier_f, result.getColumnLatestCell(family, qualifier_f).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_f)));
					put.addColumn(family, qualifier_g, result.getColumnLatestCell(family, qualifier_g).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_g)));
					put.addColumn(family, qualifier_n, result.getColumnLatestCell(family, qualifier_n).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_n)));
					put.addColumn(family, qualifier_o, result.getColumnLatestCell(family, qualifier_o).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_o)));
					put.addColumn(family, qualifier_p, result.getColumnLatestCell(family, qualifier_p).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_p)));
					put.addColumn(family, qualifier_s, result.getColumnLatestCell(family, qualifier_s).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_s)));
					put.addColumn(family, qualifier_t, result.getColumnLatestCell(family, qualifier_t).getTimestamp(), Bytes.toBytes(getValue(result, family, qualifier_t)));
					puts.add(put);
					start += length;
					context.getCounter(Counters.MigrationAmount).increment(1);
				}
			}
			table.put(puts);
			// 文件重命名
			if (fileSystem.exists(hdfsReadPath)) {
				fileSystem.delete(hdfsReadPath, true);
			}
			fileSystem.rename(hdfsWritePath, hdfsReadPath);
			LOG.info(System.currentTimeMillis());
			in.close();
			out.close();
			context.write(key, value);
			LOG.info("Mapper ended, time is [" + new Date() + "].");
		}

		@Override
		public void cleanup(Context context) {
			try {
				table.close();
				connection.close();
			} catch (IOException e) {
				LOG.info("close failly : " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		String input = "FilterJobResult"; // 垃圾文件信息存储路径
		String output = "CollectJobResult"; // 输出路径
		// System.setProperty("sun.security.krb5.debug", "true");
		CollectJob collectJob = new CollectJob();
		Configuration conf = collectJob.getConfiguration();
		conf.set("tablename", args[0]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}
		Job job = new Job(conf, JOBNAME);
		job.setJarByClass(CollectJob.class);
		job.setMapperClass(GcMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		fs.close();
		fileSystem.close();
	}
}
