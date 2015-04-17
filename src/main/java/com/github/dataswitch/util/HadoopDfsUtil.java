package com.github.dataswitch.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopDfsUtil {

	private static Logger log = LoggerFactory.getLogger(HadoopDfsUtil.class);
	// store configurations for per FileSystem schema
	private static Hashtable<String, Configuration> confs = new Hashtable<String, Configuration>();

	private static FileSystem fs;
	/**
	 * Get {@link Configuration}.
	 * 
	 * @param dir
	 *            directory path in hdfs
	 * 
	 * @param ugi
	 *            hadoop ugi
	 * 
	 * @param conf
	 *            hadoop-site.xml path
	 * 
	 * @return {@link Configuration}
	 * 
	 * @throws java.io.IOException*/

 	public static Configuration getConf(String dir, String ugi, String conf)
			throws IOException {

		Configuration cfg = null;
		URI uri = newUri(dir);
		cfg = confs.get(uri.getScheme());

		if (cfg == null) {
			cfg = newConf(ugi, conf, uri);
			confs.put(uri.getScheme(), cfg);
		}

		return cfg;
	}

	private static Configuration newConf(String ugi, String conf, URI uri) {
		Configuration cfg = new Configuration();
		cfg.setClassLoader(HadoopDfsUtil.class.getClassLoader());

		List<String> configs = newConfigOrGetDefaultConfigs(conf);

		for (String config: configs) {
			log.info(String.format("HdfsReader use %s for hadoop configuration .", config));
			cfg.addResource(new Path(config));
		}

		/* commented by bazhen.csy */
		// log.info("HdfsReader use default ugi " +
		// cfg.get(ParamsKey.HdfsReader.ugi));

		if (uri.getScheme() != null) {
			String fsname = String.format("%s://%s:%s", uri.getScheme(),uri.getHost(), uri.getPort());
			log.info("fs.default.name=" + fsname);
			cfg.set("fs.default.name", fsname);
		}
		
		if (ugi != null) {
			cfg.set("hadoop.job.ugi", ugi);
		}
		return cfg;
	}

	private static List<String> newConfigOrGetDefaultConfigs(String conf) {
		List<String> configs = new ArrayList<String>();
		if (!StringUtils.isBlank(conf) && new File(conf).exists()) {
			configs.add(conf);
		} else {
			/*
			 * For taobao internal use e.g. if bazhen.csy start a new datax
			 * job, datax will use /home/bazhen.csy/config/hadoop-site.xml
			 * as configuration xml
			 */
			String confDir = System.getenv("HADOOP_CONF_DIR");
			
			if (StringUtils.isNotBlank(confDir)) {
				//run in hadoop-0.19
				if (new File(confDir + "/hadoop-site.xml").exists()) {
					configs.add(confDir + "/hadoop-site.xml");
				} else {
					configs.add(confDir + "/core-default.xml");
					configs.add(confDir + "/core-site.xml");
				}
			}
		}
		return configs;
	}
 	
 	public static URI newUri(String dir) throws IOException {
 		URI uri = null;
		try {
			uri = new URI(dir);
			if (null == uri.getScheme()) {
				throw new IOException("HDFS Path missing scheme, check path begin with hdfs://ip:port/ .");
			}
			return uri;
		} catch (URISyntaxException e) {
			throw new IOException(e.getMessage(), e.getCause());
		}
 	}

	/**
	 * Get one handle of {@link FileSystem}.
	 * 
	 * @param dir
	 *            directory path in hdfs
	 * 
	 * @param ugi
	 *            hadoop ugi
	 * 
	 * @param configure
	 *            hadoop-site.xml path
        *
	 * @return one handle of {@link FileSystem}.
        *
	 * @throws java.io.IOException
	 * 
	 * */
 	
	public static FileSystem getFileSystem(String dir, String ugi,String configure) throws IOException {
		if (fs == null) {
			fs = FileSystem.get(getConf(dir, ugi, configure));
		}
		return fs;
	}

	public static Configuration newConf() {
		Configuration conf = new Configuration();
		/*
		 * it's weird, we need jarloader as the configuration's classloader but,
		 * I don't know what does the fucking code means Why they need the
		 * fucking currentThread ClassLoader If you know it, Pls add comment
		 * below.
		 * 
		 * private ClassLoader classLoader; { classLoader =
		 * Thread.currentThread().getContextClassLoader(); if (classLoader ==
		 * null) { classLoader = Configuration.class.getClassLoader(); } }
		 */
		conf.setClassLoader(HadoopDfsUtil.class.getClassLoader());
		
		return conf;
	}
}
