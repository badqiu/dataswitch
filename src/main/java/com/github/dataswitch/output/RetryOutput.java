package com.github.dataswitch.output;

import java.time.Duration;
import java.util.List;

import com.github.rapid.common.util.Retry;

public class RetryOutput extends ProxyOutput {
	private int retryTimes = 5;
	private int retryIntervalSeconds = 3;

	public RetryOutput() {
	}

	public RetryOutput(Output proxy) {
		super(proxy);
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		this.retryTimes = retryTimes;
	}

	public int getRetryIntervalSeconds() {
		return retryIntervalSeconds;
	}

	public void setRetryIntervalSeconds(int retryIntervalSeconds) {
		this.retryIntervalSeconds = retryIntervalSeconds;
	}

	@Override
	public void write(List<Object> rows) {

		Duration duration = Duration.ofSeconds(retryIntervalSeconds);
		Retry.retry(retryTimes, duration, () -> {
			super.write(rows);
			return null;
		});
		
	}

}
