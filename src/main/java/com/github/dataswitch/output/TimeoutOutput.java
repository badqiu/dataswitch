package com.github.dataswitch.output;

import java.time.Duration;
import java.util.List;

public class TimeoutOutput extends ProxyOutput{

	private long timeout = 0;
	
	private long _startTime = 0;
	private boolean _timeoutStatus = false;
	
	public TimeoutOutput() {
		super();
	}

	public TimeoutOutput(Output proxy) {
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
	public void write(List<Object> rows) {
		throwTimeoutExceptionIfTrue();
		
		if(_startTime == 0) {
			_startTime = System.currentTimeMillis();
		}
		
		super.write(rows);
		
		
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
