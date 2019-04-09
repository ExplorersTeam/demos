package org.exp.demos.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//检查垃圾回收前有效文件总数据量是否等于垃圾回收后大文件总数据量
public class CheckTotalDataUtil {
	private static final Logger LOG = LoggerFactory.getLogger(CheckTotalDataUtil.class);
	private static String FilterJobResult = "FilterJobResult";
	private FSDataInputStream in;
	private FileSystem fileSystem;
	private Configuration configuration;

	public CheckTotalDataUtil() throws FileNotFoundException {
		// TODO Auto-generated constructor stub
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

	public Configuration getConfiguration(){
		return configuration;
	}
	
	public boolean checkTotalData(String filename, Configuration configuration) throws IOException {
		Path filePath = new Path(filename);
		fileSystem = FileSystem.get(configuration);
		if (fileSystem.exists(filePath)) {
			long totalctdfsLength = 0;
			long totalhdfsLength = 0;
			FileStatus[] fileStatus = fileSystem.listStatus(filePath);
			for (FileStatus fileStatu : fileStatus) {
				Path subFilePath = fileStatu.getPath();
				in = fileSystem.open(subFilePath);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					String[] strs = line.split(" ");
					String[] smallfiles = strs[1].split(";");
					long ctdfsOfOneHDFS = 0;
					for (String smallfile : smallfiles) {
						String[] parts = smallfile.split(",");
						long ctdfsLength = Long.valueOf(parts[1]);
						ctdfsOfOneHDFS += ctdfsLength;
						totalctdfsLength += ctdfsLength;
					}
					Path hdfsPath = new Path(strs[0]);
					long hdfsLength = fileSystem.getFileStatus(hdfsPath).getLen();
					totalhdfsLength += hdfsLength;
					//LOG.info("对于hdfs文件[" + hdfsPath + "],有效文件长度[" + ctdfsOfOneHDFS + "],垃圾回收后总长度[" + hdfsLength + "]");
				}
				LOG.info("垃圾回收前有效ctdfs文件长度总和:" + totalctdfsLength);
				LOG.info("垃圾回收后hdfs文件长度总和:" + totalhdfsLength);
			}
			if (totalctdfsLength == totalhdfsLength) {
				return true;
			}
		} else {
			LOG.error("useful ctdfs infomation file [" + "] is not exist");
		}
		return false;
	}

	public static void main(String[] args) throws IOException {
		CheckTotalDataUtil checkTotalDataUtil = new CheckTotalDataUtil();
		Configuration configuration = checkTotalDataUtil.getConfiguration();
		boolean flag = checkTotalDataUtil.checkTotalData(FilterJobResult, configuration);
		if (flag) {
			System.out.println("垃圾回收成功");
			LOG.info("垃圾回收成功");
		} else {
			System.out.println("垃圾回收失败");
			LOG.info("垃圾回收失败");
		}
	}
}
