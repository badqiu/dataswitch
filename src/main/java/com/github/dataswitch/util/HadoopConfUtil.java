package com.github.dataswitch.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopConfUtil {
	
	private static Logger logger = LoggerFactory.getLogger(HadoopConfUtil.class);
	public static FileSystem getFileSystem(String uri,String user) throws IOException {
		Configuration conf = HadoopConfUtil.newConf();
		try {
			if(StringUtils.isBlank(uri)) {
				return FileSystem.get(conf);
			}else {
				return FileSystem.get(new URI(uri),conf,user);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("getFileSystem() error by uri"+uri+" user:"+user,e);
		} catch (URISyntaxException e) {
			throw new RuntimeException("getFileSystem() error by uri"+uri+" user:"+user,e);
		}
	}
	
	public static Configuration newConf() {
		Configuration conf = new Configuration();
		String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
		String hadoopHome = System.getenv("HADOOP_HOME");
		
		if(StringUtils.isNotBlank(hadoopConfDir)) {
			logger.info("found HADOOP_CONF_DIR env:"+hadoopConfDir+" for load core-site.xml and hdfs-site.xml");
			addResource(conf, new Path(hadoopConfDir + "/core-site.xml"));
			addResource(conf, new Path(hadoopConfDir + "/hdfs-site.xml"));
		}else if (StringUtils.isNotBlank(hadoopHome)) {
			logger.info("found HADOOP_HOME env:"+hadoopHome+" for load core-site.xml and hdfs-site.xml");
			addResource(conf, new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
			addResource(conf, new Path(hadoopHome + "/etc/hadoop/hdfs-site.xml"));
		}
		
		conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"); // The FileSystem for file: uris
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"); // The FileSystem for hdfs: uris
		return conf;
	}

	private static void addResource(Configuration conf, Path path) {
		conf.addResource(path);
		logger.info("add hadoop conf path:"+path);
	}
}
