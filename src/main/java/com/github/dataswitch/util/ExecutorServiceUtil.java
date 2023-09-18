package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.util.Assert;

public class ExecutorServiceUtil {

	private static Map<String,ExecutorService> executorServiceMap = new HashMap<String,ExecutorService>();
	
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
	

	
}
