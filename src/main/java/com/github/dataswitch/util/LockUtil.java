package com.github.dataswitch.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.util.Assert;

public class LockUtil {
	
	private static Map<String,ReentrantLock> lockMap = new HashMap<String,ReentrantLock>();
	
	public static synchronized ReentrantLock getReentrantLock(String lockGroup,String lockId) {
		Assert.hasText(lockGroup,"lockGroup must be not blank");
		Assert.hasText(lockId,"lockId must be not blank");
		
		String lockKey = lockGroup + "|" + lockId;
		ReentrantLock lock = lockMap.get(lockKey);
		if(lock == null) {
			lock = new ReentrantLock();
			lockMap.put(lockKey, lock);
		}
		return lock;
	}
	
}
