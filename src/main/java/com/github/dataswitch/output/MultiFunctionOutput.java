package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;

/**
 * 多功能的Output
 * 
 **/
public class MultiFunctionOutput extends ProxyOutput {
	private static Logger _log = LoggerFactory.getLogger(MultiFunctionOutput.class);
	
	private boolean async = false; //done
	private boolean sync = false; //done
	
	private boolean stat = false; //done
	
	private boolean lock = false; //done
	private String lockGroup = Constants.DEFAULT_LOCK_GROUP;
	private String lockId;
	private BiFunction<String, String, Lock> newLockFunction;
	
	private boolean buffered = false; // done
	private int batchSize = Constants.DEFAULT_BUFFER_SIZE;
	private int batchTimeout = Constants.DEFAULT_BUFFER_TIMEOUT;
	
	private boolean retry = false;  //done
	private int retryTimes = 0; //重试次数
	private long retryIntervalMills = RetryOutput.DEFAULT_RETRY_INTERVAL_MILLS; //重试间隔(毫秒)
	private long retryTimeoutMills = 0; //重试超时时间
	
	private boolean nullOutput = false; //done
	private boolean print = false; //done
	
	private Consumer<List<Object>> consumer; //done
	private String logger = null; //done
	
	private FailMode failMode = FailMode.FAIL_FAST;
	
	private List list = null;
	
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
	
	public void setAsync(boolean async) {
		this.async = async;
	}

	public void setSync(boolean sync) {
		this.sync = sync;
	}

	public void setLock(boolean lock) {
		this.lock = lock;
	}

	public void setLockGroup(String lockGroup) {
		this.lockGroup = lockGroup;
	}

	public void setLockId(String lockId) {
		this.lockId = lockId;
	}

	public void setNewLockFunction(BiFunction<String, String, Lock> newLockFunction) {
		this.newLockFunction = newLockFunction;
	}

	public void setBuffered(boolean buffered) {
		this.buffered = buffered;
	}

	public void setBatchSize(int bufferSize) {
		this.batchSize = bufferSize;
	}

	public void setBatchTimeout(int bufferTimeout) {
		this.batchTimeout = bufferTimeout;
	}

	public void setRetry(boolean retry) {
		this.retry = retry;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public void setRetryIntervalMills(long retryIntervalMills) {
		this.retryIntervalMills = retryIntervalMills;
	}

	public void setRetryTimeoutMills(long retryTimeoutMills) {
		this.retryTimeoutMills = retryTimeoutMills;
	}

	public void setNullOutput(boolean nullOutput) {
		this.nullOutput = nullOutput;
	}

	public void setPrint(boolean print) {
		this.print = print;
	}

	public void setStat(boolean stat) {
		this.stat = stat;
	}
	
	public List getList() {
		return list;
	}

	public void setList(List list) {
		this.list = list;
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
		
		if(list != null) {
			output = new ListOutput(list);
		}
		
		if(stat) {
			StatOutput statOutput = new StatOutput(output);
			statOutput.setPrintLog(true);
			output = statOutput;
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
		
		if(buffered) {
			output = new BufferedOutput(output,batchSize,batchTimeout);
		}
		
		
		return output;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Output newProxy = newMultiFunctionProxy(getProxy());
		_log.info("final MultiFunction outputClass:"+newProxy.getClass()+" output:"+newProxy);
		setProxy(newProxy);
		
		super.open(params);
	}
	

}
