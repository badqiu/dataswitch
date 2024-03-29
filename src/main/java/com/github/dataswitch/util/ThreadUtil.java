package com.github.dataswitch.util;

import java.time.Duration;

public class ThreadUtil {

	public static void sleep(long millis) {
		try {
			if(millis > 0)
				Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException("InterruptedException",e);
		}
	}
	
	public static void sleepSeconds(long seconds) {
		sleep(seconds * 1000);
	}
	
	public static void sleep(Duration duration) {
		sleep(duration.toMillis());
	}
	
}