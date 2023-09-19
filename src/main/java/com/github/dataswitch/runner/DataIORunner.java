package com.github.dataswitch.runner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import com.github.dataswitch.util.JVMUtil;
import com.github.dataswitch.util.ObjectUtil;
import com.github.dataswitch.util.PropertiesUtil;
import com.github.dataswitch.util.SpringConfUtil;
import com.github.dataswitch.util.Util;

import freemarker.template.Configuration;
import freemarker.template.TemplateModelException;

public class DataIORunner {
	private static Logger logger = LoggerFactory.getLogger(DataIORunner.class);
	
	private List<Beans> beansList = new ArrayList<Beans>();
	private Map<String,Object> beansMap = new HashMap<String,Object>();
	
	private String projectNamespace;
	private String projectCode;
	private String projectName;
	private String runUser;
	
	public String getProjectNamespace() {
		return projectNamespace;
	}

	public void setProjectNamespace(String projectNamespace) {
		this.projectNamespace = projectNamespace;
	}

	public String getProjectCode() {
		return projectCode;
	}

	public void setProjectCode(String projectCode) {
		this.projectCode = projectCode;
	}
	
	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	public String getRunUser() {
		return runUser;
	}

	public void setRunUser(String runUser) {
		this.runUser = runUser;
	}

	public static Map getParamsByParamsGenerator(String paramsGenerator) throws Exception {
		if(StringUtils.isBlank(paramsGenerator)) return Collections.EMPTY_MAP;
		
		try {
			int index = paramsGenerator.lastIndexOf(".");
			String clazzName = paramsGenerator.substring(0,index);
			String methodName = paramsGenerator.substring(index+1,paramsGenerator.length());
			
			Class<?> clazz = Class.forName(clazzName);
			
			Method method = clazz.getDeclaredMethod(methodName);
			Object obj = clazz.newInstance();
			return (Map)method.invoke(obj);
		}catch(Exception e) {
			throw new RuntimeException("invoke method for Map<String,Object> error by:"+paramsGenerator,e);
		}
	}
	
	public static Map getParamsByParamsFile(String paramsFile) throws IOException {
		if(StringUtils.isBlank(paramsFile)) return Collections.EMPTY_MAP;
		
		String content = FileUtils.readFileToString(new File(paramsFile));
		return PropertiesUtil.createProperties(content);
	}

	public void execByTaskId(String springConfigPath, String configPath,Map params, String taskId,String lock) throws Exception {
		if(StringUtils.isBlank(taskId)) return;
		
		taskId = taskId.trim();
		
		if(StringUtils.isNotBlank(lock)) {
			JVMUtil.lockFileForOnlyProcess(DataIORunnerMain.class.getSimpleName()+"_"+taskId);
		}
		
		parseXmlFiles(springConfigPath,configPath, params);
		long start = System.currentTimeMillis();
		execTaskById(this.beansMap,taskId,params);
		long cost = System.currentTimeMillis() - start;
		logger.info("taskId:"+taskId+" costMinute:" + (cost/1000.0/60.0)+" costSeconds:"+(cost/1000.0));
	}

	public void parseXmlFiles(String springConfigPath,String configPath, Map params) throws Exception, TemplateModelException {
		
		
		DataIOXmlParser xmlParser = new DataIOXmlParser();
		
		ApplicationContext applicationContext = SpringConfUtil.newApplicationContext(springConfigPath);
		xmlParser.setApplicationContext(applicationContext);
		
		if (applicationContext != null) {
			String APP_PROPERTIES_BEAN_NAME = "appProperties";
			if(applicationContext.containsBean(APP_PROPERTIES_BEAN_NAME)) {
				Properties properties = (Properties)applicationContext.getBean(APP_PROPERTIES_BEAN_NAME);
				if(properties != null) {
					logger.info("found properties from spring ApplicationContext, merge into SqlRunner params,properties:"+properties);
					params.putAll(properties);
				}
			}
		}
		
		try {
			if(StringUtils.isBlank(configPath)) {
				// xml from system.in
				Configuration conf = xmlParser.newConfiguration();
				Beans beans = xmlParser.fromXml(System.in, params, conf);
				beansList.add(beans);
				return;
			}
			
			
			parseXmlFilesFromPath(configPath, params, xmlParser);
		}finally {
			this.beansMap = beansList2Map(this.beansList);
		}
	}

	private void parseXmlFilesFromPath(String configPath, Map params, DataIOXmlParser xmlParser)
			throws FileNotFoundException, IOException {
		File configPathFile = ResourceUtils.getFile(configPath);
		logger.info("load sqlrunner by configPath:"+configPathFile.getAbsolutePath());
		Collection<File> files = Util.listFiles(configPathFile,"xml");
		Assert.notEmpty(files,"not found any xml config file by configPath:"+configPathFile);
		Configuration conf = xmlParser.newConfiguration();
		
		if(configPathFile.isDirectory()) {
			conf.setDirectoryForTemplateLoading(configPathFile);
		}
		
		for(File file : files) {
			if(file.isFile() && file.getName().endsWith(".xml")) {
				try {
					FileInputStream input = new FileInputStream(file);
					Beans beans = parseBeansFromInputStream(params, xmlParser, conf, file, input);
					beansList.add(beans);
				}catch(Exception e) {
					//ignore file parse error
					logger.error("parse xml file error:"+file,e);
				}
			}
		}
		
	}


	private Beans parseBeansFromInputStream(Map params, DataIOXmlParser xmlParser, Configuration conf, File file,FileInputStream input) throws Exception {
		Beans beans = xmlParser.fromXml(input, params,conf);
		beans.setSourceFile(file);
		return beans;
	}
	
	public static Map<String,Object> beansList2Map(List<Beans> beansList) {
		Map<String,Object> beansMap = new HashMap<String,Object>();
		for(Beans item : beansList) {
			ObjectUtil.afterPropertiesSetAll(item);
			beansMap.putAll(item.toBeansMap());
		}
		return beansMap;
	}

	public static void execTaskById(Map<String,Object> beans,String taskId, Map<String,Object> params) throws Exception {
		Object task = beans.get(taskId);
		Assert.notNull(task,"not found task by id:"+taskId);
		
		logger.info("execTaskById() taskId:"+taskId+" task:"+task);
		
		execTask(task,params);
	}

	private static void execTask(Object task,Map<String, Object> params) throws Exception {
		if(task instanceof Function) {
			Function<Map<String,Object>,Long> cmd = (Function)task;
			cmd.apply(params);
		}else if(task instanceof Callable) {
			Callable<Long> cmd = (Callable)task;
			cmd.call();
		}else if(task instanceof Runnable) {
			Runnable cmd = (Runnable)task;
			cmd.run();
		}else {
			throw new UnsupportedOperationException("task must imlements interface: Function<Map,Long> or Callable<Long> or Runnable. current task class:"+task.getClass());
		}
	}
	
	public void systemExit(int status) {
		System.exit(status);
	}


	
}
