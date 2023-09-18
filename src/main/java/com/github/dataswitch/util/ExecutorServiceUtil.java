package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.github.dataswitch.enums.Constants;

public class ExecutorServiceUtil {
	private static Logger logger = LoggerFactory.getLogger(ExecutorServiceUtil.class);
	private static Map<String,ExecutorService> executorServiceMap = new HashMap<String,ExecutorService>();
	
	static {
		addShutDownHook();
	}
	
	public static synchronized ExecutorService getExecutorService(String executorId,int threadPoolSize) {
		Assert.hasText(executorId,"executorId must be not blank");
		Assert.isTrue(threadPoolSize > 0,"threadPoolSize > 0 must be true");
		
		ExecutorService result = executorServiceMap.get(executorId);
		if(result == null) {
			result = Executors.newFixedThreadPool(threadPoolSize);
			executorServiceMap.put(executorId, result);
		}
		return result;
	}
	
	
	private static void addShutDownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdownAllAndAwaitTermination();
			}
		});
	}

	public static void shutdownAllAndAwaitTermination()  {
		for(Map.Entry<String,ExecutorService> entry : executorServiceMap.entrySet()) {
			try {
				ExecutorService executor = entry.getValue();
				logger.info("start shuwdown ExecutorService,executorId:"+entry.getKey()+" executorService:"+executor);
				shutdownAndAwaitTermination(executor,Constants.EXECUTOR_SERVICE_AWAIT_TERMINATION_SECOND);
			}catch(Exception e) {
				logger.error("threadPool shutdown error",e);
			}
		}
	}
	
	public static void shutdownAndAwaitTermination(ExecutorService executorService,int timeoutSeconds) throws InterruptedException {
		if(executorService != null) {
			executorService.shutdown();
			executorService.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
		}
	}
}
