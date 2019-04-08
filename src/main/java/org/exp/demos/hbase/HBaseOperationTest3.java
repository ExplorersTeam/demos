package org.exp.demos.hbase;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//删除指定rowkey的hbase记录
public class HBaseOperationTest3 {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseOperationTest3.class);
	private Configuration configuration;
	private static byte[] family = Bytes.toBytes("f");
	private static byte[] qualifier_l = Bytes.toBytes("l");

	public HBaseOperationTest3() throws FileNotFoundException {
		configuration = new Configuration();
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

	// private boolean delete(){
	//
	// }
	
	//批量删除hbase记录(按照rowkey)
	// private boolean batchDelete(){
	//
	// }
	private Table connectHbase(Configuration configuration, String tablename) throws IOException {
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf(tablename));
		return table;
	}
	

	// 按照列值删除hbase记录
	private void columnValueDelete(Configuration configuration, String tablename) throws IOException {
		Table table = connectHbase(configuration, tablename);
		Filter filter = new SingleColumnValueFilter(family, qualifier_l, CompareOp.EQUAL, Bytes.toBytes("5242880"));
		LOG.info("-----Bytes.toBytes(int)" + Bytes.toBytes(5242880));
		LOG.info("+++++Bytes.toBytes(String)" + Bytes.toBytes("5242880"));
		System.out.println("-----Bytes.toBytes(int)" + Bytes.toBytes(5242880));
		System.out.println("+++++Bytes.toBytes(String)" + Bytes.toBytes("5242880"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		ArrayList<Delete> deletes = new ArrayList<Delete>();
		for (Result result : resultScanner) {
			Delete delete = new Delete(result.getRow());
			deletes.add(delete);
		}
		table.delete(deletes);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HBaseOperationTest3 hBaseOperationTest3 = new HBaseOperationTest3();
		Configuration configuration = hBaseOperationTest3.getConfiguration();
		// String tableName = args[0];
		String tableName = "ctdfs:dfs_file";
		hBaseOperationTest3.columnValueDelete(configuration, tableName);
	}

}
