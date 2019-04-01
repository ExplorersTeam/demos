package org.exp.demos.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

public class HBaseOperationTest1 {
	private static final Log LOG = LogFactory.getLog(HBaseOperationTest1.class);
	private static final byte[] family = Bytes.toBytes("f");
	private static final byte[] qualifier_n = Bytes.toBytes("n");
	private static String hdfsUri = "hdfs://h3/apps/dfs/_fs/dlc/data/predeal/work/757/BDWAC/realtime/_dfs.132.122.1.168.26437.80.3.20181011111315";
	private static final byte[] qualifier_n_value = Bytes.toBytes(hdfsUri);

	public HBaseOperationTest1() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		InputStream coreSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/core-site.xml"));
		InputStream hdfsSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hdfs-site.xml"));
		InputStream hbaseSiteInputStream = new FileInputStream(new File("/etc/hbase/conf/hbase-site.xml"));
		configuration.addResource(coreSiteInputStream);
		configuration.addResource(hdfsSiteInputStream);
		configuration.addResource(hbaseSiteInputStream);

		Connection conection = ConnectionFactory.createConnection(configuration);
		Table table = conection.getTable(TableName.valueOf("dfs:dfs_file"));
		Scan scan = new Scan();
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier_n, CompareOp.EQUAL, qualifier_n_value);
		scan.setFilter(singleColumnValueFilter);
		LOG.info("is filter set successfully? " + scan.hasFilter());
		ResultScanner scanner = table.getScanner(scan);
		LOG.info("Got result? [" + scanner.iterator().hasNext() + "].");
		for (Result result : scanner) {
			LOG.info("Got a result, content is [" + result.toString() + "].");
		}
	}
}
