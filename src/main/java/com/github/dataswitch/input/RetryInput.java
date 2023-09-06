package com.github.dataswitch.input;

import java.time.Duration;
import java.util.List;

import org.springframework.util.Assert;

import com.github.dataswitch.util.Retry;

public class RetryInput extends ProxyInput {
	
	public static final int DEFAULT_RETRY_INTERVAL_MILLS = 3000;
	
	private int retryTimes = 0; //重试次数
	private long retryIntervalMills = DEFAULT_RETRY_INTERVAL_MILLS; //重试间隔(毫秒)
	private long retryTimeoutMills = 0; //重试超时时间

	public RetryInput() {
	}

	public RetryInput(Input proxy) {
		super(proxy);
	}

	public int getRetryTimes() {
		return retryTimes;
	}

	public void setRetryTimes(int retryTimes) {
		Assert.isTrue(retryTimes > 0 ,"retryTimes > 0 must be true");
		this.retryTimes = retryTimes;
	}

	public void setRetryIntervalMinutes(long value) {
		setRetryIntervalMills(value * 1000 * 60);
	}
	
	public void setRetryIntervalSeconds(long value) {
		setRetryIntervalMills(value * 1000);
	}

	public void setRetryInterval(Duration duration) {
		setRetryIntervalMills(duration.toMillis());
	}
	
	public void setRetryIntervalMills(long retryIntervalMills) {
		Assert.isTrue(retryIntervalMills > 0 ,"retryIntervalMills > 0 must be true");
		this.retryIntervalMills = retryIntervalMills;
	}
	
	public void setRetryTimeoutMills(long retryTimeoutMills) {
		this.retryTimeoutMills = retryTimeoutMills;
	}

	public void setRetryTimeoutSeconds(long value) {
		setRetryTimeoutMills(value * 1000);
	}
	
	public void setRetryTimeoutMinutes(long value) {
		setRetryTimeoutMills(value * 1000 * 60);
	}
	
	public void setRetryTimeout(Duration duration) {
		setRetryTimeoutMills(duration.toMillis());
	}
	
	@Override
	public List<Object> read(int size) {
		return (List)Retry.retry(retryTimes, retryIntervalMills,retryTimeoutMills, () -> {
			return super.read(size);
		});
	}

}
