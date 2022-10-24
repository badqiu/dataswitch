package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;

import com.github.dataswitch.enums.Constants;

/**
 * 多功能的Output
 * 
 **/
public class MultiFunctionOutput extends ProxyOutput {

	private boolean async = false; //done
	private boolean sync = false; //done
	
	private boolean lock = false; //done
	private String lockGroup = Constants.DEFAULT_LOCK_GROUP;
	private String lockId;
	private BiFunction<String, String, Lock> newLockFunction;
	
	private boolean buffered = false; // done
	private int bufferSize = Constants.DEFAULT_BUFFER_SIZE;
	private int bufferTimeout;
	
	private boolean retry = false;  //done
	private int retryTimes = 0; //重试次数
	private long retryIntervalMills = RetryOutput.DEFAULT_RETRY_INTERVAL_MILLS; //重试间隔(毫秒)
	private long retryTimeoutMills = 0; //重试超时时间
	
	private boolean nullOutput = false; //done
	private boolean print = false; //done
	
	private Consumer<List<Object>> consumer; //done
	private String logger = null; //done
	
	public MultiFunctionOutput() {
		super();
	}

	public MultiFunctionOutput(Output proxy) {
		setProxy(proxy);
	}
	
	@Override
	public void setProxy(Output proxy) {
		super.setProxy(proxy);
	}
	
	public void setConsumer(Consumer<List<Object>> consumer) {
		this.consumer = consumer;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	private Output newMultiFunctionProxy(Output proxy) {
		Output output = proxy;
		
		if(consumer != null) {
			output = new ComsumerOutput(consumer);
		}
		if(StringUtils.isNotBlank(logger)) {
			LoggerOutput loggerOutput = new LoggerOutput();
			loggerOutput.setLogger(logger);
			output = loggerOutput;
		}
		if(nullOutput) {
			output = new NullOutput();
		}
		if(print) {
			output = new PrintOutput();
		}
		
		if(buffered) {
			output = new BufferedOutput(output,bufferSize,bufferTimeout);
		}
		if(retry) {
			RetryOutput retryOutput = new RetryOutput(output);
			retryOutput.setRetryIntervalMills(retryIntervalMills);
			retryOutput.setRetryTimeoutMills(retryTimeoutMills);
			retryOutput.setRetryTimes(retryTimes);
			output = retryOutput;
		}
		if(lock) {
			LockOutput lockOutput = new LockOutput(output);
			lockOutput.setLockGroup(lockGroup);
			lockOutput.setLockId(lockId);
			lockOutput.setNewLockFunction(newLockFunction);
			output = lockOutput;
		}
		if(sync) {
			output = new SyncOutput(output);
		}
		if(async) {
			output = new AsyncOutput(output);
		}
		
		
		return output;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output newProxy = newMultiFunctionProxy(getProxy());
		setProxy(newProxy);
		
		super.open(params);
	}
	

}
