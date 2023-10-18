package com.github.dataswitch.input;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.github.dataswitch.output.Output;

public class TimeoutInput extends ProxyInput{

	private long timeout = 0;
	
	private long _startTime = 0;
	private boolean _timeoutStatus = false;
	
	public TimeoutInput() {
		super();
	}

	public TimeoutInput(Input proxy) {
		super(proxy);
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeoutMills) {
		this.timeout = timeoutMills;
	}
	
	public void setTimeout(Duration timeout) {
		this.timeout = timeout.toMillis();
	}
	
	public void setTimeoutSecond(long timeoutSecond) {
		this.timeout = timeoutSecond * 1000;
	}
	/**
	 * 超时设置如  5h30m30s  = 5小时30分钟30秒
	 * @param timeout
	 */
	public void setTimeout(String timeout) {
		this.timeout = Duration.parse("PT"+timeout).toMillis();
	}
	
	@Override
	public List<Map<String, Object>> read(int size) {
		throwTimeoutExceptionIfTrue();
		
		if(_startTime == 0) {
			_startTime = System.currentTimeMillis();
		}
		
		try {
			return super.read(size);
		}finally {
			checkTimeout();
		}
	}

	private void checkTimeout() {
		if(timeout > 0) {
			long cost = System.currentTimeMillis() - _startTime;
			
			if(isTimeout(cost)) {
				_timeoutStatus = true;
				throwTimeoutExceptionIfTrue();
			}
		}
	}

	protected void throwTimeoutExceptionIfTrue() {
		if(_timeoutStatus) {
			throw new RuntimeException("timeout, timeout seconds:"+(timeout/1000));
		}
	}

	private boolean isTimeout(long cost) {
		return timeout > 0 && cost >= timeout;
	}
}
