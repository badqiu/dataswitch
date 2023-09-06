package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.enums.FailMode;
import com.github.dataswitch.output.RetryOutput;
/**
 * 多功能的Input
 * @author badqiu
 *
 */
public class MultiFunctionInput extends ProxyInput{

	private static Logger _log = LoggerFactory.getLogger(MultiFunctionInput.class);
	
	private boolean async = false; //done
	private boolean sync = false; //done
	
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
	
	private boolean nullInput = false; //done
//	private boolean randomInput = false; 
	
	private Function<Integer,List<Object>> function; //done
	
	private FailMode failMode = FailMode.FAIL_FAST;
	
	
	public MultiFunctionInput() {
		super();
	}

	public MultiFunctionInput(Input proxy) {
		setProxy(proxy);
	}
	
	@Override
	public void setProxy(Input proxy) {
		super.setProxy(proxy);
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
	
	public void setNullInput(boolean nullInput) {
		this.nullInput = nullInput;
	}

	public void setFunction(Function<Integer, List<Object>> function) {
		this.function = function;
	}

	public void setFailMode(FailMode failMode) {
		this.failMode = failMode;
	}

	private Input newMultiFunctionProxy(Input proxy) {
		Input input = proxy;
		
		if(function != null) {
			input = new FunctionInput(function);
		}

		if(nullInput) {
			input = new NullInput();
		}
		
		if(retry) {
			RetryInput retryInput = new RetryInput(input);
			retryInput.setRetryIntervalMills(retryIntervalMills);
			retryInput.setRetryTimeoutMills(retryTimeoutMills);
			retryInput.setRetryTimes(retryTimes);
			input = retryInput;
		}
		
		if(lock) {
			LockInput lockInput = new LockInput(input);
			lockInput.setLockGroup(lockGroup);
			lockInput.setLockId(lockId);
			lockInput.setNewLockFunction(newLockFunction);
			input = lockInput;
		}
		if(sync) {
			input = new SyncInput(input);
		}
		if(async) {
			input = new AsyncInput(input);
		}
		
		if(buffered) {
			input = new BufferedInput(input,batchSize,batchTimeout);
		}
		
		return input;
	}
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		Input newProxy = newMultiFunctionProxy(getProxy());
		_log.info("final MultiFunction inputClass:"+newProxy.getClass()+" input:"+newProxy);
		setProxy(newProxy);
		
		super.open(params);
	}
	
}
