package com.github.dataswitch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class SpringConfUtil {
	private static Logger logger = LoggerFactory.getLogger(SpringConfUtil.class);
	
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
