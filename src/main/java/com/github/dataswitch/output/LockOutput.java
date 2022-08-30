package com.github.dataswitch.output;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.github.dataswitch.util.LockUtil;

public class LockOutput extends ProxyOutput {

	private String lockGroup = "defaultGroup";
	private String lockId;
	
	private Lock lock;
	
	public LockOutput() {
		super();
	}
	
	public LockOutput(Output proxy) {
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
	
	@Override
	public void open(Map<String, Object> params) throws Exception {
		super.open(params);
		lock = newLock(lockGroup,lockId);
	}

	protected Lock newLock(String lockGroup,String lockId) {
		return LockUtil.getReentrantLock(lockGroup, lockId);
	}
	
	@Override
	public void write(List<Object> rows) {
		try {
			lock.lock();
			super.write(rows);
		}finally {
			lock.unlock();
		}
	}
	
}
