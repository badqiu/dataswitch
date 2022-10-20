package com.github.dataswitch.support;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;

import com.github.dataswitch.BaseObject;
import com.github.dataswitch.Openable;
import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.LockUtil;

public class LockProvider extends BaseObject implements Openable {

	private String lockGroup = Constants.DEFAULT_LOCK_GROUP;
	private String lockId;
	
	private Lock lock;
	
	private BiFunction<String, String, Lock> newLockFunction;

	public String getLockGroup() {
		return lockGroup;
	}
	
	public void setLockGroup(String lockGroup) {
		this.lockGroup = lockGroup;
	}
	
	public String getLockId() {
		return lockId;
	}
	
	public void setLockId(String lockId) {
		this.lockId = lockId;
	}
	
	public Lock getLock() {
		return lock;
	}

	public void setLock(Lock lock) {
		this.lock = lock;
	}
	
	public BiFunction<String, String, Lock> getNewLockFunction() {
		return newLockFunction;
	}

	public void setNewLockFunction(BiFunction<String, String, Lock> newLockFunction) {
		this.newLockFunction = newLockFunction;
	}

	protected Lock newLock(String lockGroup,String lockId) {
		if(newLockFunction == null) {
			return LockUtil.getReentrantLock(lockGroup, lockId);
		}else {
			return newLockFunction.apply(lockGroup, lockId);
		}
	}

	@Override
	public void open(Map<String, Object> params) throws Exception {
		if(lock == null) {
			lock = newLock(lockGroup,lockId);
		}
	}
	
}
