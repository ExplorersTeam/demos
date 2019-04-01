package org.exp.demos.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileStatusDemo {
	private static final Logger LOG = LoggerFactory.getLogger(TestFileStatusDemo.class);
	private Configuration configuration;
	private static String defaultFS = "fs.defaultFS";

	public TestFileStatusDemo() {
		Configuration.addDefaultResource("core-default.xml");
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hbase-default.xml");
		Configuration.addDefaultResource("dfs-default.xml");

		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("hbase-site.xml");
		Configuration.addDefaultResource("dfs-site.xml");
		configuration = new Configuration();
	}

	private Configuration getConfiguration() {
		return configuration;
	}

	private List<String> getINodes(Path path) {
		LOG.info("-----Current path is : " + path);
		URI uri = path.toUri();
		String defaultFS = uri.getScheme() + "://" + uri.getAuthority();
		LOG.info("------defaultFS is : " + defaultFS);
		List<String> inodeList = new ArrayList<>();
		String parts[] = path.toUri().getRawPath().split(Path.SEPARATOR);
		if (parts.length == 0) {
			inodeList.add(defaultFS + "/");
		}
		for (int i = 0; i < parts.length; i++) {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(defaultFS);
			for (int j = 0; j <= i; j++) {
				stringBuilder.append(parts[j] + "/");
			}
			inodeList.add(stringBuilder.toString());
		}
		LOG.info("-----Current path's INodes number : " + inodeList.size());
		return inodeList;
	}

	public static void main(String[] args) throws IOException  {
		String pathString = args[0];
		Path path = new Path(pathString);
		TestFileStatusDemo testFileStatusDemo = new TestFileStatusDemo();
		Configuration conf = testFileStatusDemo.getConfiguration();
		LOG.info("-----core-site.xml property fs.defaultFS :" + conf.get(defaultFS));
		System.out.println("-----core-site.xml property fs.defaultFS :" + conf.get(defaultFS));
		Path defaultFSPath = new Path(conf.get(defaultFS));
		System.out.println("-----defaultFSPath : " + defaultFSPath);
		FileSystem fileSystem = defaultFSPath.getFileSystem(conf);
		List<String> inodeList = testFileStatusDemo.getINodes(path);
		for (String inode : inodeList) {
			Path inodePath = new Path(inode);
			FileStatus filestatus;
			try {
				filestatus = fileSystem.getFileStatus(inodePath);
				if (filestatus != null) {
					System.out.println("+++++pathString : " + pathString + ", filestatus : " + filestatus.toString());
				} else {
					System.out.println("*****pathString : " + pathString + ", filestatus : null ");
				}
			} catch (IOException exception) {
				System.out.println("&&&&&pathString : " + inodePath + " not exist");
				continue;
			}
		}
	}
}
