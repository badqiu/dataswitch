package com.github.dataswitch.runner;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.util.AppConfig;
import com.github.dataswitch.util.Util;

public class DataIORunnerMain {

	private static Logger logger = LoggerFactory.getLogger(DataIORunnerMain.class);
	
	
	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws Exception {
		String springConfigPath = System.getProperty("springConfigPath");
		String configPath = System.getProperty("configPath");
		String lock = System.getProperty("lock");
		String taskIdArray = System.getProperty("taskId");
		String paramsGenerator = System.getProperty("paramsGenerator");
		String paramsFile = System.getProperty("paramsFile");
		
		Assert.hasText(taskIdArray,"taskId must be not blank");

		
		Map params = DefaultParam.withDefaultParams(System.getProperties(),"day","yyyyMMdd");
		params.putAll(DataIORunner.getParamsByParamsGenerator(paramsGenerator));
		params.putAll(DataIORunner.getParamsByParamsFile(paramsFile));
		
		DataIORunner runner = buildDataIORunner(params);
		logger.info("params: configPath:"+configPath+" springConfigPath:"+springConfigPath + " taskId:"+taskIdArray);
		
		printParams(params);
		
		try {
			for(String taskId : StringUtils.split(taskIdArray,",")) {
				runner.execByTaskId(springConfigPath, configPath, params, taskId,lock);
			}
		}catch(Throwable e) {
			logger.error("task exit with error:"+e,e);
			runner.systemExit(999);
		}
		
		logger.info("EXEC END,System normal exit with 0, task:"+taskIdArray);
		runner.systemExit(0);
	}

	private static DataIORunner buildDataIORunner(Map params) {
		DataIORunner result = new DataIORunner();
		
		AppConfig appConfig = new AppConfig(params);
		result.setProjectName(appConfig.getProjectName());
		result.setProjectCode(appConfig.getProjectCode());
		result.setProjectNamespace(appConfig.getProjectNamespace());
		result.setRunUser(appConfig.getRunUser());
		
		return result;
	}

	private static String getRequiredProerpty(String key) {
		return Util.getRequiredProerpty(key);
	}

	public static void printParams(Map params) {
		params.forEach(new BiConsumer<String,Object>() {
			public void accept(String k, Object v) {
				System.out.println("[engine param] "+StringUtils.leftPad(k,30)+"  =  "+v);
			};
		});
	}
	
	
}
