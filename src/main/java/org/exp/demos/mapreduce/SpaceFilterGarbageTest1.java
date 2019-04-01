package org.exp.demos.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.exp.demos.writable.PartMetadataWritable;

import com.ctg.ctdfs.core.common.DFSConstants;
import com.google.common.collect.Maps;

public class SpaceFilterGarbageTest1 {
	private static final Log LOG = LogFactory.getLog(SpaceFilterGarbageTest1.class);
	public static final String JOBNAME1 = "FilterJob";
	public static final String JOBNAME2 = "CollectJob";
	public static final long SMALLFILELENGTH = 2097152;
	private static final String tablegcname = "ctdfs:dfs_file_gc";
	private static final String tablename = "ctdfs:dfs_file";
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
	public static enum Counters {
		ROW, HDFSUriAmount, MigrationAmount, GarbageFileAmount, SmallFileAmount
	};

	private Configuration configuration;
	private static FileSystem fileSystem;
	
	public SpaceFilterGarbageTest1() throws FileNotFoundException {
		// 通过输入流读取配置文件
		Configuration conf = new Configuration();
		InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
		InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
		InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
		InputStream yarnSiteInputStream = new FileInputStream(new File("/etc/hadoop/conf/yarn-site.xml"));
		conf.addResource(coreSiteInputStream);
		conf.addResource(hdfsSiteInputStream);
		conf.addResource(hbaseSiteInputStream);
		conf.addResource(yarnSiteInputStream);
		conf.set("mapreduce.job.user.classpath.first", "true");
		conf.set("mapreduce.task.classpath.user.precedence", "true");
		conf.set("mapred.textoutputformat.separator", " ");
		this.configuration = conf;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * 判别text对应的HDFS文件是否是垃圾文件
	 */
	private static boolean filter(String text, long sum, Configuration conf) throws IOException {
		try {
			URI uri = new URI(text);
			FileSystem fs = FileSystem.get(uri, conf);
			Path path = new Path(text);
			if (!fs.exists(path)) {
				return false;
			}
			FileStatus filestatus = fs.getFileStatus(path);
			long HDFSFileLength = filestatus.getLen();
			if (HDFSFileLength == 0) {
				return false;
			}
			DecimalFormat df = new DecimalFormat("0.000");
			LOG.info("HDFSFile : " + text + ", sum is : " + sum + ", HDFSFileLength is : " + HDFSFileLength);
			double result = Double.valueOf(df.format((float) sum / (float) HDFSFileLength));
			if (result < 0.2) {
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
	public static boolean issmallfile(long length, String isdir) {
		if (length < SMALLFILELENGTH && isdir.equals("false")) {
			return true;
		}
		return false;
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

	public static Map<String, String> toMap(String status, String ctime, String datafile, long offset, long length, String fileinfo, String isdir, String owner, String group, String permission) {
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

	private static String getValue(Result result, byte[] family, byte[] qualifier) {
		Cell cell = result.getColumnLatestCell(family, qualifier);
		String value = Bytes.toString(CellUtil.cloneValue(cell));
		return value;
	}

	static class GcMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private Configuration configuration = null;
		Connection connection = null;
		Table table = null;
		Table tablegc = null;
		// map key=gabage hdfsuri value=dfsinfo
		@Override
		public void setup(Context context) throws IOException {
			System.setProperty("sun.security.krb5.debug", "true");
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
			LOG.info("sun.security.krb5.debug:" + System.getProperty("sun.security.krb5.debug"));
			LOG.info("javax.security.auth.useSubjectCredsOnly:" + System.getProperty("javax.security.auth.useSubjectCredsOnly"));
			configuration = context.getConfiguration();
			LOG.info("-----hbase.regionserver.kerberos.principal:" + configuration.get("hbase.regionserver.kerberos.principal"));
			LOG.info("-----hbase.master.kerberos.principal:" + configuration.get("hbase.master.kerberos.principal"));
			LOG.info("-----yarn的属性:" + configuration.get("yarn.timeline-service.principal"));
			connection = ConnectionFactory.createConnection(configuration);
			table = connection.getTable(TableName.valueOf(tablename));
			LOG.info("Table name is [" + table.getName().getNameAsString() + "].");
			tablegc = connection.getTable(TableName.valueOf(tablegcname));
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
			// FileSystem fs = hdfsReadPath.getFileSystem(configuration);
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
				try {
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
				} catch (Exception exception) {
					LOG.error("catch exception:" + exception.getMessage());
					break;
				}
			}
			tablegc.put(puts);
			// 文件重命名
			//fs.delete(hdfsReadPath, true);
			//fs.rename(hdfsWritePath, hdfsReadPath);
			LOG.info(System.currentTimeMillis());
			in.close();
			out.close();
			context.write(key, value);
			LOG.info("Mapper ended, time is [" + new Date() + "].");
		}

		@Override
		public void cleanup(Context context) {
			try {
				LOG.info("-----close table&tablegc&connection&fileSystem");
				table.close();
				tablegc.close();
				connection.close();
				// fileSystem.close();
			} catch (IOException e) {
				LOG.info("close failly : " + e.getMessage());
			}
		}
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
					context.write(new Text(hdfsFileUriStr), columns);
				}
			}
		}
	}

	static class FilterReducer extends Reducer<Text, Result, Text, Text> {
		@Override
		public void reduce(Text text, Iterable<Result> smallfiles, Context context) throws IOException, InterruptedException {
			TreeSet<PartMetadataWritable> treeSet = new TreeSet<>();
			LOG.info("treeSet");
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			for (Result smallfile : smallfiles) {
				// ......此处需要添加判断，将hdfs uri、startIndex、length相同的小文件剔除
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
			if (filter(text.toString(), sum, context.getConfiguration())) {
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text, new Text(sb.toString()));
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// tablegcname = "ctdfs:dfs_file_gc";
		// tablename = "ctdfs:dfs_file";
		String output1 = "GarbageFilterResult";
		String output2 = "GarbageCollectResult";
		System.setProperty("sun.security.krb5.debug", "true");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		SpaceFilterGarbageTest1 spaceFilterGarbageTest1 = new SpaceFilterGarbageTest1();
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
		Configuration conf = spaceFilterGarbageTest1.getConfiguration();
		// 每次运行程序之前删除运行结果的输出文件夹
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output1))) {
			fs.delete(new Path(output1), true);
		}
		if (fs.exists(new Path(output2))) {
			fs.delete(new Path(output2), true);
		}

		// job1:获取垃圾hdfsuri
		Job job1 = new Job(conf, JOBNAME1);
		job1.setJarByClass(SpaceFilterGarbageTest1.class);
		TableMapReduceUtil.initTableMapperJob(tablename, scan, FilterMapper.class, Text.class, Result.class, job1);
		job1.setReducerClass(FilterReducer.class);
		job1.setNumReduceTasks(1);
		// 设置job1的执行结果输出路径
		Path path1 = new Path(output1);
		FileOutputFormat.setOutputPath(job1, path1);
		// job2:回收垃圾hdfsuri
		Job job2 = new Job(conf, JOBNAME2);
		job2.setJarByClass(SpaceFilterGarbageTest1.class);
		job2.setMapperClass(GcMapper.class);
		FileInputFormat.addInputPath(job2, path1);
		Path path2 = new Path(output2);
		FileOutputFormat.setOutputPath(job2, path2);
		// Job控制器:将Job1加入控制器
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);
		// job2加入控制器
		ControlledJob ctrljob2 = new ControlledJob(conf);
		ctrljob2.setJob(job2);
		// 设置多个作业直接的依赖关系：job2的启动，依赖于job1作业的完成
		// job1的输出路径path1是job2的输入路径
		ctrljob2.addDependingJob(ctrljob1);
		// 主控制器
		JobControl jobCtrl = new JobControl("myctrl");
		jobCtrl.addJob(ctrljob1);
		jobCtrl.addJob(ctrljob2);
		Thread t = new Thread(jobCtrl);
		t.start();
		while (true) {
			if (jobCtrl.allFinished()) {// 如果作业成功完成，就打印成功作业的信息
				System.out.println(jobCtrl.getSuccessfulJobList());
				System.out.println("所有作业执行完毕");
				jobCtrl.stop();
				break;
			}
			if (jobCtrl.getFailedJobList().size() > 0) {
				System.out.println(jobCtrl.getFailedJobList());
				System.out.println("有作业执行失败");
				jobCtrl.stop();
				break;
			}
		}
		LOG.info("not FileSystem close");
		// fileSystem.close();
		// fs.close();
	}
}
