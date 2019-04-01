package org.exp.demos.mapreduce;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.Date;
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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ctg.ctdfs.core.common.DFSConstants;
import com.ctg.ctdfs.core.common.DFSContext;
import com.google.common.collect.Maps;

public class SpaceFilterGarbage {
	private static final Log LOG = LogFactory.getLog(SpaceFilterGarbage.class);
	public static final String JOBNAME1 = "SpaceFilterJob";
	public static final String JOBNAME2 = "SpaceCollectJob";
	public static String HDFSFileName;
	public static final long SMALLFILELENGTH = 2097152;
	public static enum Counters {ROW,HDFSUriAmount,MigrationAmount,GarbageFileAmount,SmallFileAmount};

	
	public static long HDFSFileLength;
	
	public static Configuration conf;
	public static FileSystem fs;
	public static URI uri;
	public static Path path;
	public static FileStatus filestatus;
	public static FSDataInputStream fs_in;
	public static FSDataOutputStream fs_out;
	private static DFSContext dfsContext;
	
	public SpaceFilterGarbage(){
		
	}
	
	/**
	 * 判别text对应的HDFS文件是否是垃圾文件
	 */
	public static boolean filter(String text, long sum) throws IOException {
		try {
			uri = new URI(text);
			conf = new Configuration();
			fs = FileSystem.get(uri, conf);
			path=new Path(text);
			if (!fs.exists(path)) {
				return false;
			}
			filestatus = fs.getFileStatus(path);
			HDFSFileLength = filestatus.getLen();
			if (HDFSFileLength == 0) {
				return false;
			}
			DecimalFormat df = new DecimalFormat("0.000");
			LOG.info("HDFSFile : " + text + ", sum is : " + sum + ", HDFSFileLength is : " + HDFSFileLength);
			double result = Double.valueOf(df.format((float) sum / (float) HDFSFileLength));
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

	/**
	 * 垃圾文件有效数据拷贝
	 */
	private static int migrationData(FSDataInputStream inputStream, FSDataOutputStream outputStream, Long position, int length) {
		byte[] buffer = new byte[length];
		int offset = 0;
		int readBytes = 0;
		try {
			readBytes = inputStream.read(position, buffer, offset, length);
			outputStream.write(buffer);
			LOG.info("the bytes has been readed is : " + readBytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return readBytes;
	}

	static class GcMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		byte[] qualifier_i = Bytes.toBytes("i");//the startindex of dfs file in hdfs file
		byte[] qualifier_n = Bytes.toBytes("n");
		byte[] qualifier_s = Bytes.toBytes("s");
		byte[] qualifier_t = Bytes.toBytes("t");
		byte[] qualifier_f = Bytes.toBytes("f");
		byte[] qualifier_d = Bytes.toBytes("d");
		byte[] qualifier_o = Bytes.toBytes("o");
		byte[] qualifier_g = Bytes.toBytes("g");
		byte[] qualifier_p = Bytes.toBytes("p");

		String tableName = "dfs:dfs_file";

		//map key=gabage hdfsuri value=dfsinfo
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// // 通过job的context获取Configuration对象
			// Configuration conf = context.getConfiguration();
			// LOG.info("ZooKeeper quorum is [" +
			// conf.get("hbase.zookeeper.quorum") + "].");
			// 操作HDFS文件
			String datafile = value.toString().trim();
			LOG.info("datafile uri is : " + datafile);
			// Path hdfsReadPath = new Path(datafile);
			// Path hdfsWritePath = new Path(datafile +".temp");
			// FileSystem fs = hdfsReadPath.getFileSystem(conf);
			// FSDataInputStream in = fs.open(hdfsReadPath);
			// FSDataOutputStream out = fs.create(hdfsWritePath, true);//
			// temp文件覆盖写
			// // 根据hdfsUri获取其对应的HBase记录(利用过滤器获取)
			// Connection connection = ConnectionFactory.createConnection(conf);
			// Table table = connection.getTable(TableName.valueOf(tableName));
			// LOG.info("Table name is [" + table.getName().getNameAsString() +
			// "].");
			// // do not use hbaseClient,use SingleColumnValueFilter get the
			// useful
			// // record
			// Scan scan = new Scan();
			// scan.setFilter(new SingleColumnValueFilter(family, qualifier_n,
			// CompareOp.EQUAL, datafile.getBytes()));
			// LOG.debug("is filter set successfully? " + scan.hasFilter());
			// ResultScanner results = table.getScanner(scan);
			// LOG.info("Got result? [" + results.iterator().hasNext() + "].");
			// for (Result result : results) {
			// LOG.info("Got a result, content is [" + result.toString() +
			// "].");
			// }

			LOG.info(System.currentTimeMillis());
			// in.close();
			// out.close();
			// results.close();
			// table.close();
			// connection.close();
			// fs.close();
			context.write(new Text(datafile), NullWritable.get());
			LOG.info("Mapper ended, time is [" + new Date() + "].");
		}
	}

	static class GCReducer extends Reducer<Text, NullWritable, Text, Text> {
		String tableName = "dfs:dfs_file";
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");

		@Override
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
			// 通过job的context获取Configuration对象
			Configuration conf = context.getConfiguration();
			String datafile = key.toString().trim();
			LOG.info("datafile is : " + datafile);
			// 获取垃圾大文件中有效小文件元数据的过程(操作hbase dfs:dfs_file表)
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			LOG.info("Table name is [" + table.getName().getNameAsString() + "].");
			// do not use hbaseClient,use SingleColumnValueFilter get the useful
			// record
			Scan scan = new Scan();
			SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier_n, CompareOp.EQUAL, datafile.getBytes());
			scan.setFilter(singleColumnValueFilter);
			LOG.info("is filter set successfully? " + scan.hasFilter());
			ResultScanner results = table.getScanner(scan);
			LOG.info("Got result? [" + results.iterator().hasNext() + "].");
			for (Result result : results) {
				LOG.info("Got a result, content is [" + result.toString() + "].");
			}
			context.write(new Text(datafile), new Text("success"));
			results.close();
			table.close();
			connection.close();
			LOG.info("GCReducer ended, time is [" + new Date() + "].");
		}
	}
	
	static class FilterMapper extends TableMapper<Text,Result>{
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");//the hdfs file uri
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		byte[] qualifier_d = Bytes.toBytes("d");//is dir or not of dfs file
		byte[] qualifier_i = Bytes.toBytes("i");//the startindex of dfs file in hdfs file
		/*
		 * map()获取HBase表中的小文件数据
		 * the useful data:
		 * 1、hdfs file uri
		 * 2、the effective data : startindex and endindex、length
		 */
		@Override
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.ROW).increment(1);
			/*
			 * 1、获取HDFSFileUri、SmallFileName、SmallFileLength
			 * 2、以HDFSFileUri为key，SmallFileName构成的ArrayList为value
			 * 
			 */
			Cell hdfsFileUri = columns.getColumnLatestCell(family, qualifier_n);
			Cell fileLength = columns.getColumnLatestCell(family, qualifier_l);
			Cell isDir = columns.getColumnLatestCell(family, qualifier_d);
			if(hdfsFileUri!=null && fileLength!=null && isDir!=null ){
				String hdfsFileUriStr = Bytes.toString(CellUtil.cloneValue(hdfsFileUri));
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength))) ;
				String isdir = Bytes.toString(CellUtil.cloneValue(isDir));
				if(issmallfile(length,isdir)){
					context.getCounter(Counters.SmallFileAmount).increment(1);
					//......此处需要添加判断，将hdfs uri、startIndex、length相同的小文件剔除
					context.write(new Text(hdfsFileUriStr), columns);
				}
			}
		}
	}
	/**
	 * @author lsg
	 *	FilterReducer的输出<key,value>=<hdfsuri,List<DFSFileStatus>>
	 *	
	 */
	static class FilterReducer extends Reducer<Text, Result, Text, Text> {
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		
		@Override
		public void reduce(Text text,Iterable<Result> smallfiles,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			long sum = 0;
			StringBuilder sb = new StringBuilder();
			for(Result smallfile:smallfiles){
				//......此处需要添加判断，将hdfs uri、startIndex、length相同的小文件剔除
				Cell fileLength = smallfile.getColumnLatestCell(family, qualifier_l);
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				sum += length;
				sb.append(smallfile.toString());
			}	
			if(filter(text.toString(),sum)){
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text, new Text(""));
			}
		}
	}
	
	
public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		String tablename = "dfs:dfs_file";
		String output1 = "GarbageHDFSFile";
		String output2 = "Result";
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
		// 通过输入流读取配置文件
		InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
		InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
		InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
		conf.addResource(coreSiteInputStream);
		conf.addResource(hdfsSiteInputStream);
		conf.addResource(hbaseSiteInputStream);
		conf.set("mapreduce.job.user.classpath.first", "true");
		conf.set("mapreduce.task.classpath.user.precedence", "true");
		// String krbPath = "/etc/krb5.cof";
		// System.setProperty("java.security.krb5.conf", krbPath);

		// 使用UserGroupInformation进行Kerberos认证
		// UserGroupInformation.setConfiguration(conf);
		// final String user = "dfs/h3a1.ecloud.com";
		// final String keyPath = "/etc/security/keytabs/dfs.app.keytab";
		// try {
		// UserGroupInformation.loginUserFromKeytab(user, keyPath);
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// LOG.info("hbase.regionserver.kerberos.principal is : " +
		// conf.get("hbase.regionserver.kerberos.principal"));

		//每次运行程序之前删除运行结果的输出文件夹
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output1))){
			fs.delete(new Path(output1),true);
		}
		if (fs.exists(new Path(output2))) {
			fs.delete(new Path(output2), true);
		}
		fs.close();
		// job1:获取垃圾hdfsuri
		Job job1 = new Job(conf, JOBNAME1);
		job1.setJarByClass(SpaceFilterGarbage.class);
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				FilterMapper.class,
				Text.class,
				Result.class,
				job1
				);
		job1.setReducerClass(FilterReducer.class);
		job1.setNumReduceTasks(1);
		// 设置job1的执行结果输出路径
		Path path1 = new Path(output1);
		FileOutputFormat.setOutputPath(job1, path1);
		// job2:回收垃圾hdfsuri
		Job job2 = new Job(conf, JOBNAME2);
		job2.setJarByClass(SpaceFilterGarbage.class);
		job2.setMapperClass(GcMapper.class);
		job2.setReducerClass(GCReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(NullWritable.class);
		job2.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job2, path1);
		Path path2 = new Path(output2);
		FileOutputFormat.setOutputPath(job2, path2);
		// Job控制器:将Job1加入控制器
		ControlledJob ctrljob1 = new ControlledJob(conf);
		ctrljob1.setJob(job1);
		//job2加入控制器
		ControlledJob ctrljob2=new ControlledJob(conf);   
        ctrljob2.setJob(job2); 
		// 设置多个作业直接的依赖关系：job2的启动，依赖于job1作业的完成
		// job1的输出路径path1是job2的输入路径
        ctrljob2.addDependingJob(ctrljob1);
		// 主控制器
        JobControl jobCtrl=new JobControl("myctrl");
        jobCtrl.addJob(ctrljob1);   
        jobCtrl.addJob(ctrljob2); 
		Thread t = new Thread(jobCtrl);
        t.start();   
        while(true){   
        	if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息   
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
	}
}
