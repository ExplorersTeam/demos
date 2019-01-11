package org.exp.demos.mapreduce;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ctg.ctdfs.core.common.DFSConstants;
import com.google.common.collect.Maps;


public class ChainRowkeyFilterGarbage {
	private static final Log LOG = LogFactory.getLog(ChainRowkeyFilterGarbage.class);
	public static final String JOBNAME="FilterJob";
	public static final long SMALLFILELENGTH = 2097152;
	public static enum Counters {ROW,HDFSUriAmount,MigrationAmount,GarbageFileAmount,SmallFileAmount};

	
	public static long hdfsFileLength;
	
	public static Configuration conf;
	public static FileSystem fs;
	public static URI uri;
	public static Path path;
	public static FileStatus filestatus;
	public static FSDataInputStream fs_in;
	public static FSDataOutputStream fs_out;
	
	private static Connection connection;
	private static HTable dfsFileTable = null;
	
//	
	public ChainRowkeyFilterGarbage(){
		
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
			filestatus = fs.getFileStatus(path);
			hdfsFileLength = filestatus.getLen();
			float result = ((float)sum/(float)hdfsFileLength);
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
	
	static class FilterMapper extends TableMapper<Text,Result>{
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_n = Bytes.toBytes("n");//the hdfs file uri
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		byte[] qualifier_d = Bytes.toBytes("d");//is dir or not of dfs file
		byte[] qualifier_i = Bytes.toBytes("i");//the startindex of dfs file in hdfs file
		
		@Override
		public void map(ImmutableBytesWritable rowkey,Result columns,Context context) throws IOException, InterruptedException{
			context.getCounter(Counters.ROW).increment(1);
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
	static class FilterReducer extends Reducer<Text, Result, Text, Text>{	
		byte[] family = Bytes.toBytes("f");
		byte[] qualifier_l = Bytes.toBytes("l");//the length of dfs file
		
		@Override
		public void reduce(Text text,Iterable<Result> smallfiles,Context context) throws IOException, InterruptedException{
			//text=one hdfsuri,smallfiles=dfs files belong to this hdfsuri
			context.getCounter(Counters.HDFSUriAmount).increment(1);
			long sum = 0;
			for(Result smallfile:smallfiles){
				//......此处需要添加判断，将hdfs uri、startIndex、length相同的小文件剔除
				Cell fileLength = smallfile.getColumnLatestCell(family, qualifier_l);
				long length = Long.valueOf(Bytes.toString(CellUtil.cloneValue(fileLength)));
				sum += length;
			}	
			
			if(filter(text.toString(),sum)){	
				context.getCounter(Counters.GarbageFileAmount).increment(1);
				context.write(text, new Text("success"));	
			}
		}
	}
	//指定family、qualifier、value，获取resultscanner
	public static ResultScanner getResults(Table table,byte[] family, byte[] qualifier,String datafile) throws IOException{
		SingleColumnValueFilter gabageHdfsFilter = 
				new SingleColumnValueFilter(family,qualifier,CompareOp.EQUAL,Bytes.toBytes(datafile));
		gabageHdfsFilter.setFilterIfMissing(true);
		Scan scan = new Scan();
		scan.setFilter(gabageHdfsFilter);
		ResultScanner results = table.getScanner(scan);
		LOG.info("out-----"+results.toString());
		return results;
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
    
	// put，会覆盖原有值
	public static void put(Table table, String rowStr, Map<String, String> columns) throws IOException {
			Put put = new Put(Bytes.toBytes(rowStr));
			for (Entry<String, String> data : columns.entrySet()) {
				put.add(DFSConstants.META_COLUMN_FAMILY_BYTES, Bytes.toBytes(data.getKey()), data.getValue() == null ? null : Bytes.toBytes(data.getValue()));
			}
			table.put(put);
	}

	static class GcMapper extends Mapper<Text,Text,Text,Text>{
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
		Map<String,String> rowDataMap = Maps.newHashMap(); 
		

		//map key=gabage hdfsuri value=dfsinfo
		@Override
		public void map(Text key,Text smallfiles,Context context) throws IOException, InterruptedException{
			String datafile = key.toString();
			Path hdfsReadPath = new Path(datafile);
			Path hdfsWritePath = new Path(datafile +".temp");
			//在每个map里实例化Configuration
			Configuration conf = new Configuration();
			// conf.addResource("/etc/hbase/conf/core-site.xml");
			// conf.addResource("/etc/hbase/conf/hbase-site.xml");
			// conf.addResource("/etc/hbase/conf/hdfs-site.xml");
			InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
			InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
			InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
			// if (coreSiteInputStream != null) {
			// byte[] buffer = new byte[1024];
			// StringBuffer sBuffer = new StringBuffer();
			// while (coreSiteInputStream.read() != -1) {
			// coreSiteInputStream.read(buffer);
			// sBuffer.append(Bytes.toString(buffer));
			// }
			// LOG.info("coreSite file content is :" + "\n" +
			// sBuffer.toString());
			// }
			conf.addResource(coreSiteInputStream);
			conf.addResource(hdfsSiteInputStream);
			conf.addResource(hbaseSiteInputStream);

			LOG.info("ZooKeeper quorum is [" + conf.get("hbase.zookeeper.quorum") + "].");
			fs = hdfsReadPath.getFileSystem(conf);
			FSDataInputStream in = null;
			in = fs.open(hdfsReadPath);
			FSDataOutputStream out = fs.create(hdfsWritePath,true);//temp文件覆盖写
			Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(tableName));
			LOG.info("Table name is [" + table.getName().getNameAsString() + "].");
			//do not use hbaseClient,use SingleColumnValueFilter get the useful record			
			Scan scan = new Scan();
			scan.setFilter(new SingleColumnValueFilter(family, qualifier_t, CompareOp.EQUAL, datafile.getBytes()));
			ResultScanner results = table.getScanner(scan);
			LOG.info("Got result? [" + results.iterator().hasNext() + "].");
			for(Result result:results){
				LOG.info("Got a result, content is [" + result.toString() + "].");
			}
			LOG.info(System.currentTimeMillis());

			in.close();
			out.close();
			results.close();
			table.close();
			connection.close();
			LOG.info("Mapper ended, time is [" + new Date() + "].");
		}
	}


	
public static void main(String []args) throws ClassNotFoundException, IOException, InterruptedException{
		String tablename = "dfs:dfs_file";
		String output = "GarbageHDFSFile";
		Scan scan = new Scan();

		Configuration conf = new Configuration();
		//每次运行程序之前删除运行结果的输出文件夹
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output),true);
		}
		fs.close();
		
		Job job = Job.getInstance(conf, JOBNAME);
		job.setJarByClass(ChainRowkeyFilterGarbage.class);
		TableMapReduceUtil.initTableMapperJob(
				tablename,
				scan,
				FilterMapper.class,
				Text.class,
				Result.class,
				job
				);
		ChainReducer.setReducer(job, FilterReducer.class, Text.class, Result.class, Text.class, Text.class, conf);
		System.out.println("FilterReducer已完成......");
		ChainReducer.addMapper(job, GcMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		System.out.println("GcMapper已完成.......");
		
		job.setNumReduceTasks(1);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
	}
}
