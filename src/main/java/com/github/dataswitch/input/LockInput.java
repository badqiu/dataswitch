package com.github.dataswitch.input;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.github.dataswitch.enums.Constants;
import com.github.dataswitch.util.LockUtil;

public class LockInput extends ProxyInput {

	private String lockGroup = Constants.DEFAULT_LOCK_GROUP;
	private String lockId;
	
	private Lock lock;
	
	public LockInput() {
		super();
	}
	
	public LockInput(Input proxy) {
		super(proxy);
	}
	
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

	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		if(lock == null) {
			lock = newLock(lockGroup,lockId);
		}
	}

	protected Lock newLock(String lockGroup,String lockId) {
		return LockUtil.getReentrantLock(lockGroup, lockId);
	}

	@Override
	public List<Object> read(int size) {
		try {
			lock.lock();
			return super.read(size);
		}finally {
			lock.unlock();
		}
	}
	
}
