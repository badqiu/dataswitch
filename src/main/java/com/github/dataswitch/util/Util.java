package com.github.dataswitch.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

public class Util {

	private static Logger logger = LoggerFactory.getLogger(Util.class);
	
	public static String[] splitColumns(String columns) {
		if(org.apache.commons.lang.StringUtils.isBlank(columns)) return null;
		
		String[] result = StringUtils.trimArrayElements(columns.trim().split("[,，\\s]+"));
		return result;
	}
	
	public static Collection oneToList(Object row)  {
		if(row == null) return null;
		
		if(row instanceof Collection) {
			return (Collection)row;
		}else if(row.getClass().isArray()) {
			Object[] array = (Object[])row;
			return Arrays.asList(array);
		}else {
			return Arrays.asList(row);
		}
	}
	
	public static Object first(Object row)  {
		if(row == null) return null;
		
		if(row instanceof Collection) {
			Collection col = (Collection)row;
			if(col.isEmpty()) return null;
			return col.iterator().next();
		}else if(row.getClass().isArray()) {
			Object[] array = (Object[])row;
			if(array.length == 0) return null;
			return array[0];
		}else {
			return row;
		}
	}
	
	public static Function toFunction(Supplier supplier) {
		return new Function() {
			public Object apply(Object size) {
				return supplier.get();
			}
		};
	}
	
	public static Function toFunction(Callable callable) {
		return new Function() {
			public Object apply(Object size) {
				try {
					return callable.call();
				} catch (Exception e) {
					throw new RuntimeException("callable error",e);
				}
			}
		};
	}
	
	public static long getTPS(long loopCount,long costTimeMills) {
		if(costTimeMills <= 0) return 0;
		
		long tps = loopCount * 1000 / costTimeMills;
		return tps;
	}
	

	public static boolean getBooleanProerpty(String key,boolean defaultValue) {
		String value = System.getProperty(key);
		if(org.apache.commons.lang.StringUtils.isBlank(value)) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}
	
	public static int getIntProerpty(String key,int defaultValue) {
		String value = System.getProperty(key);
		if(org.apache.commons.lang.StringUtils.isBlank(value)) {
			return defaultValue;
		}
		return Integer.parseInt(value);
	}
	
	public static String getRequiredProerpty(String key) {
		String value = System.getProperty(key);
		Assert.hasText(value,"not found required system property:"+key);
		return value;
	}
	
	public static String getRequiredEnv(String key) {
		String value = System.getenv(key);
		Assert.hasText(value,"not found required env:"+key);
		return value;
	}

	public static Collection<File> listFiles(String configDir,String extension) throws FileNotFoundException {
		File dir = ResourceUtils.getFile(configDir);
		Assert.notNull(dir,"not found dir:"+configDir);
		return listFiles(dir,extension);
	}

	public static Collection<File> listFiles(File dir,String extension) {
		try {
			if(dir.isFile()) return Arrays.asList(dir);
			Collection<File> files = FileUtils.listFiles(dir, new String[]{extension}, true);
			return  files;
		}catch(Exception e) {
			throw new RuntimeException("listFiles error,path:"+dir+" extension:"+extension,e);
		}
	}
	
	public static ApplicationContext newApplicationContext(
			String springConfigDir) {
		logger.info("newApplicationContext by springConfigDir:"+springConfigDir);
		long start = System.currentTimeMillis();
		ApplicationContext applicationContext = null;
		if(org.apache.commons.lang.StringUtils.isNotBlank(springConfigDir)) {
			if(springConfigDir.startsWith("classpath")) {
				applicationContext = new ClassPathXmlApplicationContext(springConfigDir);
			}else {
				applicationContext = new FileSystemXmlApplicationContext(springConfigDir);
			}
		}
		logger.info("newApplicationContext cost timeMills:" + (System.currentTimeMillis() - start));
		return applicationContext;
	}
}
