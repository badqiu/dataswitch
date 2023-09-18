package com.github.dataswitch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class SpringConfUtil {
	private static Logger logger = LoggerFactory.getLogger(SpringConfUtil.class);
	
	public static ApplicationContext newApplicationContext(
			String springConfigPath) {
		logger.info("newApplicationContext by springConfigPath:"+springConfigPath);
		
		long start = System.currentTimeMillis();
		ApplicationContext applicationContext = null;
		if(org.apache.commons.lang.StringUtils.isNotBlank(springConfigPath)) {
			try {
				Class clazz = Class.forName(springConfigPath);
				applicationContext = new AnnotationConfigApplicationContext(clazz);
			} catch (ClassNotFoundException e) {
				//ignore
				
				if(springConfigPath.startsWith("classpath")) {
					applicationContext = new ClassPathXmlApplicationContext(springConfigPath);
				}else {
					applicationContext = new FileSystemXmlApplicationContext(springConfigPath);
				}
			}
		}
		long cost = System.currentTimeMillis() - start;
		logger.info("newApplicationContext cost timeMills:" + cost + " applicationContext.class:"+applicationContext.getClass()+ " applicationName:"+applicationContext.getApplicationName());
		 
		return applicationContext;
	}
	
}
