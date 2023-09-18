package com.github.dataswitch.support;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.springframework.util.Assert;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Openable;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.ExecutorServiceUtil;
import com.github.dataswitch.util.TableName;
/**
 * 提供线程池ExecutorService的创建
 * 
 * @author badqiu
 *
 */
public class ExecutorServiceProvider extends BaseObject implements Openable,AutoCloseable,TableName{
	
	private int threadPoolSize = Constants.EXECUTOR_SERVICE_THREAD_POOL_SIZE;
	
	private String executorGroup = Constants.DEFAULT_EXECUTOR_GROUP;
	private String executorName = "defaultExecutor";
	
	private BiFunction<String, Integer, ExecutorService> newExecutorServiceFunction;

	private ExecutorService executorService = null;
	
	private boolean shutdownExecutorServiceOnClose = false;
	
	public ExecutorServiceProvider() {
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		createExecutorServiceIfNull();
	}

	@Override
	public void close() throws Exception {
		if(shutdownExecutorServiceOnClose) {
			if(executorService != null) {
				executorService.shutdown();
				executorService.awaitTermination(Constants.EXECUTOR_SERVICE_AWAIT_TERMINATION_SECOND, TimeUnit.SECONDS);
			}
		}
	}
	
	protected void createExecutorServiceIfNull() {
		if(executorService == null) {
			Assert.hasText(executorGroup,"executorGroup must be not blank");
			Assert.hasText(executorName,"executorName must be not blank");
			String executorId = executorGroup + "|" + executorName;
			executorService = newExecutorService(executorId,threadPoolSize);
		}
	}
	
	protected ExecutorService newExecutorService(String executorId,int threadPoolSize) {
		if(newExecutorServiceFunction == null) {
			return ExecutorServiceUtil.getExecutorService(executorId, threadPoolSize);
		}else {
			return newExecutorServiceFunction.apply(executorId, threadPoolSize);
		}
	}
	
	public int getThreadPoolSize() {
		return threadPoolSize;
	}


	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}


	public String getExecutorGroup() {
		return executorGroup;
	}


	public void setExecutorGroup(String executorGroup) {
		this.executorGroup = executorGroup;
	}


	public String getExecutorName() {
		return executorName;
	}


	public void setExecutorName(String executorName) {
		this.executorName = executorName;
	}


	public ExecutorService getExecutorService() {
		return executorService;
	}

	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}

	public void setTable(String table) {
		setExecutorName(table);
	}
	
	@Override
	public String getTable() {
		return getExecutorName();
	}

	public BiFunction<String, Integer, ExecutorService> getNewExecutorServiceFunction() {
		return newExecutorServiceFunction;
	}

	public void setNewExecutorServiceFunction(BiFunction<String, Integer, ExecutorService> newExecutorServiceFunction) {
		this.newExecutorServiceFunction = newExecutorServiceFunction;
	}

	public boolean isShutdownExecutorServiceOnClose() {
		return shutdownExecutorServiceOnClose;
	}

	public void setShutdownExecutorServiceOnClose(boolean shutdownExecutorServiceOnClose) {
		this.shutdownExecutorServiceOnClose = shutdownExecutorServiceOnClose;
	}

	
}
